import ws from 'k6/ws';
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const ROOMS        = parseInt(__ENV.ROOMS          || '1', 10);
const USERS        = parseInt(__ENV.USERS          || '2', 10);
const RAMP_S       = parseInt(__ENV.RAMP           || '150', 10);
const SETTLE_S     = parseInt(__ENV.SETTLE          || '5', 10);
const DURATION_S   = parseInt(__ENV.DURATION       || '30', 10);
const DRAIN_S      = parseInt(__ENV.DRAIN          || '30', 10);
const SEND_EVERY_S = parseFloat(__ENV.SEND_INTERVAL || '20');
const HTTP_BASE    = __ENV.HTTP_BASE || 'http://localhost:80';
const WS_BASE      = __ENV.WS_BASE   || HTTP_BASE.replace(/^http/, 'ws');
const WS_ERR_TOL_PCT = parseFloat(__ENV.WS_ERR_TOLERANCE_PCT || '0.5');

const MODE     = (__ENV.MODE || 'tput').toLowerCase();
const SENDING  = MODE !== 'conn' && SEND_EVERY_S > 0;

const TOTAL_VUS = ROOMS * USERS;
const SETTLE_PHASE_S = SENDING ? Math.max(0, SETTLE_S) : 0;
const DRAIN_PHASE_S = SENDING ? Math.max(0, DRAIN_S) : 0;
const SEND_START_MS = (RAMP_S + SETTLE_PHASE_S) * 1000;
const SEND_END_MS = SEND_START_MS + DURATION_S * 1000;
const SOCKET_CLOSE_MS = SEND_END_MS + DRAIN_PHASE_S * 1000;
const CLOSE_BUFFER_S = 1;
const scenarioElapsedMs = () => Date.now() - exec.scenario.startTime;
const scenarioStages = [
  { duration: `${RAMP_S}s`, target: TOTAL_VUS },
];
if (SETTLE_PHASE_S > 0) scenarioStages.push({ duration: `${SETTLE_PHASE_S}s`, target: TOTAL_VUS });
scenarioStages.push({ duration: `${DURATION_S}s`, target: TOTAL_VUS });
if (DRAIN_PHASE_S > 0) scenarioStages.push({ duration: `${DRAIN_PHASE_S}s`, target: TOTAL_VUS });
scenarioStages.push({ duration: `${CLOSE_BUFFER_S}s`, target: 0 });

const WS_ERR_BUDGET = Math.max(5, Math.ceil(TOTAL_VUS * WS_ERR_TOL_PCT / 100));

const e2eLatency = new Trend('msg_e2e_latency', true);
const msgsSent   = new Counter('msgs_sent');
const msgsRecv   = new Counter('msgs_received');
const wsErrors   = new Counter('ws_errors');

export const options = {
  scenarios: {
    chat: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: scenarioStages,
      gracefulRampDown: '30s',
      gracefulStop:     '90s',
    },
  },
  thresholds: MODE === 'conn'
    ? {
        checks:        ['rate>0.99'],
        ws_connecting: ['p(95)<1000'],
        ws_errors:     [`count<=${WS_ERR_BUDGET}`],
      }
    : {
        checks:          ['rate>0.99'],
        ws_connecting:   ['p(95)<1000'],
        msg_e2e_latency: ['p(95)<2500', 'p(99)<3500'],
        ws_errors:       [`count<=${WS_ERR_BUDGET}`],
        msgs_sent:       ['count>0'],
        msgs_received:   ['count>0'],
      },
};

const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };

export function setup() {
  const stamp = Date.now();

  const rooms = [];
  for (let i = 0; i < ROOMS; i++) {
    const res = http.post(`${HTTP_BASE}/api/rooms`,
      JSON.stringify({ name: `load-${stamp}-room-${i}` }), JSON_HEADERS);
    check(res, { 'room created': (r) => r.status === 201 });
    rooms.push(res.json('id'));
  }

  const users = [];
  for (let i = 0; i < USERS; i++) {
    const res = http.post(`${HTTP_BASE}/api/users`,
      JSON.stringify({ username: `load-${stamp}-user-${i}` }), JSON_HEADERS);
    check(res, { 'user created': (r) => r.status === 201 });
    users.push(res.json('id'));
  }

  console.log(`setup: ${rooms.length} rooms, ${users.length} users, ${TOTAL_VUS} VUs`);
  return {
    rooms,
    users,
    mode: MODE,
    ramp_s: RAMP_S,
    settle_s: SETTLE_PHASE_S,
    duration_s: DURATION_S,
    drain_s: DRAIN_PHASE_S,
    send_interval_s: SENDING ? SEND_EVERY_S : 0,
  };
}

export default function (data) {
  if (exec.vu.iterationInScenario > 0) {
    sleep(1);
    return;
  }

  const idx    = exec.vu.idInTest - 1;
  const roomId = data.rooms[Math.floor(idx / USERS)];
  const userId = data.users[idx % USERS];
  const url    = `${WS_BASE}/ws?room=${roomId}`;

  const remainingMs = Math.max(1000, SOCKET_CLOSE_MS - scenarioElapsedMs());

  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', function () {
      if (!SENDING) return;
      const rampRemaining = Math.max(0, SEND_START_MS - scenarioElapsedMs());
      const firstSendDelay = rampRemaining + Math.random() * SEND_EVERY_S * 1000;
      const sendMessage = function () {
        if (scenarioElapsedMs() >= SEND_END_MS) return;
        socket.send(JSON.stringify({ user_id: userId, body: `t=${Date.now()}` }));
        msgsSent.add(1);
      };
      socket.setTimeout(function () {
        sendMessage();
        socket.setInterval(sendMessage, SEND_EVERY_S * 1000);
      }, firstSendDelay);
    });

    socket.on('message', function (raw) {
      msgsRecv.add(1);
      try {
        const m = JSON.parse(raw);
        const match = /t=(\d+)/.exec(m.body || '');
        if (match) e2eLatency.add(Date.now() - parseInt(match[1], 10));
      } catch (_) {}
    });

    socket.on('error', function () {
      wsErrors.add(1);
    });

    socket.setTimeout(function () {
      socket.close();
    }, remainingMs);
  });

  check(res, { 'ws handshake 101': (r) => r && r.status === 101 });
  if (!res || res.status !== 101) {
    wsErrors.add(1);
    sleep(1);
  }
}
