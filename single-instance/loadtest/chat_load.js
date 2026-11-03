// k6 load test for the chat-s websocket server.
//
// One VU == one user == one long-lived websocket connection. Connections ramp
// up over RAMP seconds, settle before sending, send only during DURATION, then
// stay open for DRAIN seconds so recipients can observe pending deliveries.
// Each VU records delivery latency for its own echo and other room messages.
//
// MODE selects the limit under test (see run-limits.ps1):
//   MODE=conn — hold sockets, send nothing; sweep the socket count to find the
//               max concurrent ws an instance holds.
//   MODE=tput — fix the socket count, raise the message rate (small/fractional
//               SEND_INTERVAL) to find the max message rate at that count.
//
// The soft ramp matters: opening N sockets simultaneously swamps the accept
// backlog and most handshakes get reset, which looks like a server ceiling but
// is really a thundering herd. Ramping in lets the server admit connections at
// a sustainable rate so the numbers reflect real capacity.
//
// Parametrized by env vars so a single script covers the whole scenario matrix
// (see run-matrix.ps1). Peak VUs = ROOMS * USERS; throughput runs take about
// RAMP + SETTLE + DURATION + DRAIN seconds, plus a short ramp-down.
//
//   k6 run -e ROOMS=10 -e USERS=5 -e RAMP=150 -e SETTLE=5 -e DURATION=30 -e DRAIN=30 chat_load.js
//
// Defaults are the smallest scenario (1 room, 2 users) so a bare `k6 run` works.

import ws from 'k6/ws';
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// ---- Parameters ------------------------------------------------------------
const ROOMS        = parseInt(__ENV.ROOMS          || '1', 10);    // number of rooms
const USERS        = parseInt(__ENV.USERS          || '2', 10);    // users per room
const RAMP_S       = parseInt(__ENV.RAMP           || '150', 10);  // ramp connections up over N seconds
const SETTLE_S     = parseInt(__ENV.SETTLE          || '5', 10);    // wait for all room members to connect
const DURATION_S   = parseInt(__ENV.DURATION       || '30', 10);   // hold-at-full-load length, seconds
const DRAIN_S      = parseInt(__ENV.DRAIN          || '30', 10);   // keep sockets open while pending deliveries drain
const SEND_EVERY_S = parseFloat(__ENV.SEND_INTERVAL || '20');      // one message / N seconds; <=0 disables sending
const HTTP_BASE    = __ENV.HTTP_BASE || 'http://localhost:80';
const WS_BASE      = __ENV.WS_BASE   || HTTP_BASE.replace(/^http/, 'ws');
const WS_ERR_TOL_PCT = parseFloat(__ENV.WS_ERR_TOLERANCE_PCT || '0.5'); // ws_errors tolerated as teardown noise, % of sockets

// MODE picks which limit we're hunting:
//   conn  — hold sockets, send nothing (SEND_INTERVAL forced to 0): find the
//           max concurrent ws the instance holds.
//   tput  — fix the socket count, raise the message rate (small SEND_INTERVAL):
//           find the max message rate at that connection count.
// Default is tput so a bare `k6 run` keeps the original send-and-measure behavior.
const MODE     = (__ENV.MODE || 'tput').toLowerCase();
const SENDING  = MODE !== 'conn' && SEND_EVERY_S > 0;

const TOTAL_VUS = ROOMS * USERS;
const SETTLE_PHASE_S = SENDING ? Math.max(0, SETTLE_S) : 0;
const DRAIN_PHASE_S = SENDING ? Math.max(0, DRAIN_S) : 0;
const SEND_START_MS = (RAMP_S + SETTLE_PHASE_S) * 1000;
const SEND_END_MS = SEND_START_MS + DURATION_S * 1000;
const SOCKET_CLOSE_MS = SEND_END_MS + DRAIN_PHASE_S * 1000;
const CLOSE_BUFFER_S = 1;
const scenarioStages = [
  { duration: `${RAMP_S}s`, target: TOTAL_VUS },
];
if (SETTLE_PHASE_S > 0) scenarioStages.push({ duration: `${SETTLE_PHASE_S}s`, target: TOTAL_VUS });
scenarioStages.push({ duration: `${DURATION_S}s`, target: TOTAL_VUS });
if (DRAIN_PHASE_S > 0) scenarioStages.push({ duration: `${DRAIN_PHASE_S}s`, target: TOTAL_VUS });
scenarioStages.push({ duration: `${CLOSE_BUFFER_S}s`, target: 0 });

// k6 tears a few sockets down uncleanly at end-of-test, so a handful of ws_errors
// surfaces even on a healthy step. Tolerate a budget proportional to socket count
// (a real collapse is orders of magnitude above it); floor at 5 for tiny runs.
// Mirrors summarize.ps1's -WsErrTolerancePct so the runner gate and read-off agree.
const WS_ERR_BUDGET = Math.max(5, Math.ceil(TOTAL_VUS * WS_ERR_TOL_PCT / 100));

// ---- Custom metrics --------------------------------------------------------
const e2eLatency = new Trend('msg_e2e_latency', true); // ms; includes the relay's ~2s poll
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
  // Thresholds are the per-cell pass/fail gate (they drive k6's exit code, which
  // the runner uses to flag a breached step). They differ by MODE: the conn
  // sweep carries no traffic, so message thresholds would spuriously fail.
  thresholds: MODE === 'conn'
    ? {
        // Connection-limit gate: every handshake succeeds, ws errors within the
        // teardown budget, and sockets connect promptly. The largest step that
        // stays green is the ws ceiling for this memory budget.
        checks:        ['rate>0.99'],   // 'ws handshake 101'
        ws_connecting: ['p(95)<1000'],
        ws_errors:     [`count<=${WS_ERR_BUDGET}`],
      }
    : {
        checks:          ['rate>0.99'],
        ws_connecting:   ['p(95)<1000'],
        // Delivery latency is dominated by the relay's poll interval (~2s); these
        // bounds assume the default 2s poll. Loosen if you raise pollInterval.
        // For the tput sweep, judge saturation by completeness (see summarize.ps1)
        // — unbounded latency growth is the tell, not the 2s floor.
        msg_e2e_latency: ['p(95)<2500', 'p(99)<3500'],
        // ws errors within the teardown budget, and traffic actually flowed both ways.
        ws_errors:       [`count<=${WS_ERR_BUDGET}`],
        msgs_sent:       ['count>0'],
        msgs_received:   ['count>0'],
      },
};

const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };

// setup pre-creates the rooms and users (FK constraints require them to exist
// before any message references them) and hands their UUIDs to every VU.
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
  // One long-lived socket per VU. `ramping-vus` is a *looping* executor, so when a
  // VU's socket closes at end-of-window it would immediately start a second
  // iteration and reconnect — and that reconnect races the scenario shutdown, gets
  // reset, and is wrongly counted as a failed `ws handshake 101` (this is what made
  // the high-VU cells look like a ~47% server ceiling; it isn't). Connect once.
  if (exec.vu.iterationInScenario > 0) {
    sleep(1);
    return;
  }

  const idx    = exec.vu.idInTest - 1;                 // 0-based, unique across the test
  const roomId = data.rooms[Math.floor(idx / USERS)];  // USERS consecutive VUs share a room
  const userId = data.users[idx % USERS];
  const url    = `${WS_BASE}/ws?room=${roomId}`;

  // Keep every socket open through the send window and drain window, then close
  // it before k6 starts its final ramp-down.
  const remainingMs = Math.max(1000, SOCKET_CLOSE_MS - exec.instance.currentTestRunDuration);

  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', function () {
      if (!SENDING) return; // conn mode: just hold the socket open, send nothing
      const rampRemaining = Math.max(0, SEND_START_MS - exec.instance.currentTestRunDuration);
      const firstSendDelay = rampRemaining + Math.random() * SEND_EVERY_S * 1000;
      const sendMessage = function () {
        if (exec.instance.currentTestRunDuration >= SEND_END_MS) return;
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
      } catch (_) { /* ignore frames that aren't our messages */ }
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
