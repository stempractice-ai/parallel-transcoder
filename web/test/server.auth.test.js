import { test } from "node:test";
import assert from "node:assert/strict";
import WebSocket from "ws";
import { writeFile } from "node:fs/promises";
import path from "node:path";

import { startServer, startServerExpectingExit, req } from "../test-helpers/server.js";

const KEY = "test-api-key";
const ADMIN = "test-admin-key";

test("refuses to start outside desktop mode when no API key is configured", { timeout: 30_000 }, async () => {
  const { code, stderr } = await startServerExpectingExit({ env: {} });
  assert.equal(code, 1, `expected exit 1, got ${code}`);
  assert.match(stderr, /TRANSCODER_API_KEY is required/);
});

test("desktop mode starts and serves without any key configured", async () => {
  // The desktop app binds loopback only and has no operator to hold a key.
  const s = await startServer({ env: { DESKTOP_MODE: "1" } });
  try {
    assert.equal((await req(s.url, "/api/jobs")).status, 200);
    // The shipped SPA is the same file in the desktop build; it reads this
    // flag to decide whether to prompt for a key and whether it may open a
    // WebSocket at all.
    assert.equal((await req(s.url, "/api/health")).json.authRequired, false);
  } finally {
    await s.stop();
  }
});

test("desktop mode websockets work with no auth frame at all", async () => {
  // Without this, gating the client's WS connect on a stored key would
  // silently remove live job progress from the desktop build.
  const s = await startServer({ env: { DESKTOP_MODE: "1", TRANSCODER_WS_AUTH_TIMEOUT_MS: "250" } });
  try {
    const out = await wsOutcome(s.wsUrl, { send: { type: "subscribe", jobId: "none" }, settleMs: 900 });
    assert.deepEqual(out, { state: "open" });
  } finally {
    await s.stop();
  }
});

test("API routes are closed without a key and open with one", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    assert.equal((await req(s.url, "/api/jobs")).status, 401);
    assert.equal((await req(s.url, "/api/jobs", { headers: { "X-API-Key": "wrong" } })).status, 401);
    assert.equal((await req(s.url, "/api/jobs", { headers: { "X-API-Key": KEY } })).status, 200);
  } finally {
    await s.stop();
  }
});

test("the api_key query parameter no longer authenticates", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    // Keys in query strings land in access logs and Referer headers.
    const r = await req(s.url, `/api/jobs?api_key=${KEY}`);
    assert.equal(r.status, 401);
  } finally {
    await s.stop();
  }
});

test("health is unauthenticated and leaks no host detail", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    const r = await req(s.url, "/api/health");
    assert.equal(r.status, 200);
    assert.deepEqual(r.json, { status: "ok", localMode: false, authRequired: true });
  } finally {
    await s.stop();
  }
});

test("readiness is unauthenticated and reports ready with no object store", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    const r = await req(s.url, "/api/ready");
    assert.equal(r.status, 200);
    assert.equal(r.json.ready, true);
  } finally {
    await s.stop();
  }
});

test("an unreachable object store is reported but does not flip readiness", { timeout: 30_000 }, async () => {
  // The deployment runs one replica, so failing readiness on a remote
  // dependency would pull the only backend and take down the UI, job list and
  // downloads — none of which need the object store.
  const s = await startServer({
    env: {
      TRANSCODER_API_KEY: KEY,
      // Nothing is listening here, so HeadBucket cannot succeed.
      OBJECT_STORE_URL: "http://127.0.0.1:1",
      OBJECT_STORE_BUCKET: "transcoder-segments",
      AWS_ACCESS_KEY_ID: "test",
      AWS_SECRET_ACCESS_KEY: "test",
      AWS_REGION: "us-east-1",
    },
  });
  try {
    const r = await req(s.url, "/api/ready");
    assert.equal(r.status, 200, "a dead bucket must not unready the only replica");
    assert.equal(r.json.ready, true);
    assert.equal(r.json.objectStore, false, "the degraded dependency must still be visible");
  } finally {
    await s.stop();
  }
});

test("capabilities carries the platform detail health used to expose, behind auth", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    assert.equal((await req(s.url, "/api/capabilities")).status, 401);
    const r = await req(s.url, "/api/capabilities", { headers: { "X-API-Key": KEY } });
    assert.equal(r.status, 200);
    assert.equal(r.json.platform, process.platform);
    assert.equal(r.json.localMode, false);
  } finally {
    await s.stop();
  }
});

test("localMode reflects a present coordinator binary", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY }, localMode: true });
  try {
    assert.deepEqual((await req(s.url, "/api/health")).json, { status: "ok", localMode: true, authRequired: true });
  } finally {
    await s.stop();
  }
});

test("admin routes need the admin key even with a valid API key", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY, TRANSCODER_ADMIN_KEY: ADMIN } });
  try {
    const apiOnly = { "X-API-Key": KEY };
    assert.equal((await req(s.url, "/api/cluster/workers", { headers: apiOnly })).status, 403);
    assert.equal(
      (await req(s.url, "/api/cluster/workers", { headers: { ...apiOnly, "X-Admin-Key": "wrong" } })).status,
      403,
    );
    assert.equal(
      (await req(s.url, "/api/jobs", { method: "DELETE", headers: apiOnly })).status,
      403,
    );
    const scale = await req(s.url, "/api/cluster/workers/scale", {
      method: "POST",
      headers: { ...apiOnly, "Content-Type": "application/json" },
      body: JSON.stringify({ replicas: 3 }),
    });
    assert.equal(scale.status, 403);
  } finally {
    await s.stop();
  }
});

test("the correct admin key gets past authorization", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY, TRANSCODER_ADMIN_KEY: ADMIN } });
  try {
    const headers = { "X-API-Key": KEY, "X-Admin-Key": ADMIN };
    const r = await req(s.url, "/api/cluster/workers", { headers });
    // Off-cluster there is no Kubernetes API, so 200-with-unavailable or 503 are
    // both fine; what matters is that authorization no longer blocks.
    assert.notEqual(r.status, 403);
    const del = await req(s.url, "/api/jobs", { method: "DELETE", headers });
    assert.equal(del.status, 200);
  } finally {
    await s.stop();
  }
});

test("admin routes fail closed when no admin key is configured", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    const r = await req(s.url, "/api/cluster/workers", { headers: { "X-API-Key": KEY } });
    assert.equal(r.status, 403);
    assert.deepEqual(r.json, { error: "Admin key not configured" });
  } finally {
    await s.stop();
  }
});

// A download is an <a href> navigation, which cannot carry a header. Rather
// than reopening the key to query strings, the UI trades its key for a
// short-lived, single-use ticket bound to one file.
test("downloads are refused without a key or ticket", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    const r = await req(s.url, "/api/download/nosuch/out.mp4");
    assert.equal(r.status, 401);
  } finally {
    await s.stop();
  }
});

test("a download ticket is single-use, file-bound and expires", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY }, localMode: true, files: { "a.mp4": "x" } });
  try {
    // Minting requires the API key.
    assert.equal(
      (await req(s.url, "/api/download-ticket", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ jobId: "nosuch", filename: "out.mp4" }),
      })).status,
      401,
    );

    const minted = await req(s.url, "/api/download-ticket", {
      method: "POST",
      headers: { "X-API-Key": KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ jobId: "nosuch", filename: "out.mp4" }),
    });
    // No such job, so minting must not succeed.
    assert.equal(minted.status, 404);

    // A forged ticket never works.
    const forged = await req(s.url, "/api/download/nosuch/out.mp4?ticket=deadbeef");
    assert.equal(forged.status, 401);
  } finally {
    await s.stop();
  }
});

test("a valid ticket downloads the file once and is then spent", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY }, localMode: true, files: { "a.mp4": "x" } });
  try {
    const started = await req(s.url, "/api/transcode", {
      method: "POST",
      headers: { "X-API-Key": KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ uploadId: "a.mp4", format: "mp4" }),
    });
    assert.equal(started.status, 200);
    const jobId = started.json.jobId;

    // The stub coordinator produces nothing, so plant an output to fetch.
    await writeFile(path.join(s.stateDir, "outputs", jobId, "output.mp4"), "payload");

    const minted = await req(s.url, "/api/download-ticket", {
      method: "POST",
      headers: { "X-API-Key": KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ jobId, filename: "output.mp4" }),
    });
    assert.equal(minted.status, 200);
    const ticket = minted.json.ticket;
    assert.equal(typeof ticket, "string");

    // A ticket is bound to its file: it must not unlock a different one.
    assert.equal((await req(s.url, `/api/download/${jobId}/other.mp4?ticket=${ticket}`)).status, 401);

    const got = await req(s.url, `/api/download/${jobId}/output.mp4?ticket=${ticket}`);
    assert.equal(got.status, 200);
    assert.equal(got.text, "payload");

    // Single use.
    assert.equal((await req(s.url, `/api/download/${jobId}/output.mp4?ticket=${ticket}`)).status, 401);
  } finally {
    await s.stop();
  }
});

/** Open a socket and resolve with how it ended, or "open" if it survived. */
function wsOutcome(url, { send, settleMs = 1200 } = {}) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    let closed = null;
    ws.on("open", () => {
      if (send !== undefined) ws.send(typeof send === "string" ? send : JSON.stringify(send));
      setTimeout(() => {
        if (closed) return;
        ws.close();
        resolve({ state: "open" });
      }, settleMs);
    });
    ws.on("close", (code) => {
      closed = code;
      resolve({ state: "closed", code });
    });
    ws.on("error", (e) => { if (!closed) reject(e); });
  });
}

test("a websocket that never authenticates is closed with 4401", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY, TRANSCODER_WS_AUTH_TIMEOUT_MS: "250" } });
  try {
    assert.deepEqual(await wsOutcome(s.wsUrl), { state: "closed", code: 4401 });
  } finally {
    await s.stop();
  }
});

test("a websocket presenting the wrong key is closed with 4401", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY, TRANSCODER_WS_AUTH_TIMEOUT_MS: "250" } });
  try {
    assert.deepEqual(await wsOutcome(s.wsUrl, { send: { type: "auth", key: "wrong" } }), { state: "closed", code: 4401 });
  } finally {
    await s.stop();
  }
});

test("a websocket that subscribes before authenticating is closed with 4401", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY, TRANSCODER_WS_AUTH_TIMEOUT_MS: "250" } });
  try {
    const out = await wsOutcome(s.wsUrl, { send: { type: "subscribe", jobId: "anything" } });
    assert.deepEqual(out, { state: "closed", code: 4401 });
  } finally {
    await s.stop();
  }
});

test("a websocket that authenticates stays open and may then subscribe", { timeout: 20_000 }, async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY, TRANSCODER_WS_AUTH_TIMEOUT_MS: "250" } });
  try {
    const out = await new Promise((resolve, reject) => {
      const ws = new WebSocket(s.wsUrl);
      let acked = false;
      let done = false;
      const finish = (v) => { if (done) return; done = true; try { ws.close(); } catch { /* already closing */ } resolve(v); };
      // Without this deadline the promise never settles when the server sends
      // no ack at all, which is exactly the pre-fix behaviour.
      const deadline = setTimeout(() => finish({ state: "no-ack", acked }), 5_000);
      ws.on("open", () => ws.send(JSON.stringify({ type: "auth", key: KEY })));
      ws.on("message", (d) => {
        let msg;
        try { msg = JSON.parse(d.toString()); } catch { return; }
        if (msg.type !== "auth-ok") return;
        acked = true;
        ws.send(JSON.stringify({ type: "subscribe", jobId: "none" }));
        setTimeout(() => { clearTimeout(deadline); finish({ state: "open", acked }); }, 600);
      });
      ws.on("close", (code) => { clearTimeout(deadline); if (!acked) finish({ state: "closed", code }); });
      ws.on("error", (e) => { clearTimeout(deadline); if (!done) { done = true; reject(e); } });
    });
    assert.deepEqual(out, { state: "open", acked: true });
  } finally {
    await s.stop();
  }
});
