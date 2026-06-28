// k6 load test for the chat-s websocket server.
//
// One VU == one user == one long-lived websocket connection. Connections ramp
// up softly (RAMP seconds) instead of all opening on the same tick, then hold
// at full load for DURATION seconds. Each VU sends one message every
// SEND_INTERVAL seconds and records the end-to-end delivery latency of every
// message it receives (its own echo plus every other member's messages in the
// same room).
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
// (see run-matrix.ps1). Peak VUs = ROOMS * USERS; total wall-clock ~= RAMP +
// DURATION (+ a short graceful stop).
//
//   k6 run -e ROOMS=10 -e USERS=5 -e RAMP=150 -e DURATION=30 chat_load.js
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
const DURATION_S   = parseInt(__ENV.DURATION       || '30', 10);   // hold-at-full-load length, seconds
const SEND_EVERY_S = parseFloat(__ENV.SEND_INTERVAL || '20');      // one message / N seconds; <=0 disables sending
const HTTP_BASE    = __ENV.HTTP_BASE || 'http://localhost:80';
const WS_BASE      = __ENV.WS_BASE   || HTTP_BASE.replace(/^http/, 'ws');

// MODE picks which limit we're hunting:
//   conn  — hold sockets, send nothing (SEND_INTERVAL forced to 0): find the
//           max concurrent ws the instance holds.
//   tput  — fix the socket count, raise the message rate (small SEND_INTERVAL):
//           find the max message rate at that connection count.
// Default is tput so a bare `k6 run` keeps the original send-and-measure behavior.
const MODE     = (__ENV.MODE || 'tput').toLowerCase();
const SENDING  = MODE !== 'conn' && SEND_EVERY_S > 0;

const TOTAL_VUS = ROOMS * USERS;
const ACTIVE_MS = (RAMP_S + DURATION_S) * 1000; // when the active window ends and every socket should close

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
      stages: [
        { duration: `${RAMP_S}s`,     target: TOTAL_VUS }, // soft ramp: open sockets gradually
        { duration: `${DURATION_S}s`, target: TOTAL_VUS }, // hold at full load
      ],
      gracefulRampDown: '0s', // sockets self-close at the end of the window (see below)
      gracefulStop:     '15s',
    },
  },
  // Thresholds are the per-cell pass/fail gate (they drive k6's exit code, which
  // the runner uses to flag a breached step). They differ by MODE: the conn
  // sweep carries no traffic, so message thresholds would spuriously fail.
  thresholds: MODE === 'conn'
    ? {
        // Connection-limit gate: every handshake succeeds, no ws errors, and
        // sockets connect promptly. The largest step that stays green is the
        // ws ceiling for this memory budget.
        checks:        ['rate>0.99'],   // 'ws handshake 101'
        ws_connecting: ['p(95)<1000'],
        ws_errors:     ['count==0'],
      }
    : {
        checks:          ['rate>0.99'],
        ws_connecting:   ['p(95)<1000'],
        // Delivery latency is dominated by the relay's poll interval (~2s); these
        // bounds assume the default 2s poll. Loosen if you raise pollInterval.
        // For the tput sweep, judge saturation by completeness (see summarize.ps1)
        // — unbounded latency growth is the tell, not the 2s floor.
        msg_e2e_latency: ['p(95)<2500', 'p(99)<3500'],
        // No websocket errors, and traffic actually flowed both ways.
        ws_errors:       ['count==0'],
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
  return { rooms, users };
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

  // A VU opens its socket when it ramps in and holds it until the active window
  // ends, so every connection closes together regardless of when it opened.
  // This keeps it one long-lived socket per VU (no reconnect churn during hold)
  // while still ramping the connection count up smoothly.
  const remainingMs = Math.max(1000, ACTIVE_MS - exec.instance.currentTestRunDuration);

  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', function () {
      if (!SENDING) return; // conn mode: just hold the socket open, send nothing
      // Random initial offset so VUs don't all fire on the same tick.
      socket.setTimeout(function () {
        socket.setInterval(function () {
          socket.send(JSON.stringify({ user_id: userId, body: `t=${Date.now()}` }));
          msgsSent.add(1);
        }, SEND_EVERY_S * 1000);
      }, Math.random() * SEND_EVERY_S * 1000);
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
