import ws from 'k6/ws';
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// ---- Parameters ------------------------------------------------------------
const ROOMS        = parseInt(__ENV.ROOMS          || '1', 10);    // number of rooms
const USERS        = parseInt(__ENV.USERS          || '2', 10);    // users per room
const RAMP_S       = parseInt(__ENV.RAMP           || '150', 10);  // ramp connections up over N seconds
const DURATION_S   = parseInt(__ENV.DURATION       || '30', 10);   // hold-at-full-load length, seconds
const SEND_EVERY_S = parseFloat(__ENV.SEND_INTERVAL || '20');      // one message / N seconds; <=0 disables sending
const HTTP_BASE    = __ENV.HTTP_BASE || 'http://localhost:80';
const WS_BASE      = __ENV.WS_BASE   || HTTP_BASE.replace(/^http/, 'ws');
const WS_ERR_TOL_PCT = parseFloat(__ENV.WS_ERR_TOLERANCE_PCT || '0.5'); // ws_errors tolerated as teardown noise, % of sockets


const MODE     = (__ENV.MODE || 'tput').toLowerCase();
const SENDING  = MODE !== 'conn' && SEND_EVERY_S > 0;

const TOTAL_VUS = ROOMS * USERS;
const ACTIVE_MS = (RAMP_S + DURATION_S) * 1000;

const WS_ERR_BUDGET = Math.max(5, Math.ceil(TOTAL_VUS * WS_ERR_TOL_PCT / 100));

const e2eLatency = new Trend('msg_e2e_latency', true); // ms; ~2s outbox poll dominates (+ a small Redis pub/sub hop)
const msgsSent   = new Counter('msgs_sent');
const msgsRecv   = new Counter('msgs_received');
const wsErrors   = new Counter('ws_errors');

export const options = {
  scenarios: {
    chat: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: `${RAMP_S}s`,     target: TOTAL_VUS }, // soft ramp: open sockets gradually
        { duration: `${DURATION_S}s`, target: TOTAL_VUS }, // hold at full load
      ],
      gracefulRampDown: '30s',
      gracefulStop:     '90s',
    },
  },
  thresholds: MODE === 'conn'
    ? {
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

  const rooms = [];
  for (let i = 0; i < ROOMS; i++) {
    rooms.push(i);
  }

  const users = [];
  for (let i = 0; i < USERS; i++) {
    users.push(i);
  }

  console.log(`setup: ${rooms.length} rooms, ${users.length} users, ${TOTAL_VUS} VUs`);
  return { rooms, users };
}

export default function (data) {
  if (exec.vu.iterationInScenario > 0) {
    sleep(1);
    return;
  }

  //find limit for open conn
  const idx    = exec.vu.idInTest - 1;                 // 0-based, unique across the test
  const roomId = data.rooms[Math.floor(idx / USERS)];  // USERS consecutive VUs share a room
  const userId = data.users[idx % USERS];
  const url    = `${WS_BASE}/ws?client_id=${userId}&room_ids=${roomId}`;
  const remainingMs = Math.max(1000, ACTIVE_MS - exec.instance.currentTestRunDuration);

  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', function () {
      if (!SENDING) return; // conn mode: just hold the socket open, send nothing
      // Random initial offset so VUs don't all fire on the same tick.
      socket.setTimeout(function () {
        socket.setInterval(function () {
            socket.send(JSON.stringify({
            room_id: roomId,
            payload: {
                client_id: userId,
                body: `t=${Date.now()}`,
            },
        }));
          msgsSent.add(1);
        }, SEND_EVERY_S * 1000);
      }, Math.random() * SEND_EVERY_S * 1000);
    });

    socket.on('error', function () {
      wsErrors.add(1);
    });

    // End the session: close the socket, which ends the VU iteration.
    socket.setTimeout(function () {
      socket.close();
    }, remainingMs);
  });

  check(res, { 'ws handshake 101': (r) => r && r.status === 101 });
  if (!res || res.status !== 101) {
    wsErrors.add(1);
    sleep(1); // back off so a failing handshake doesn't hot-loop into a reconnect storm
  }
}