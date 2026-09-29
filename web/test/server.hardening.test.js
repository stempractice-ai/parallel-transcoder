import { test } from "node:test";
import assert from "node:assert/strict";
import { readdir } from "node:fs/promises";
import path from "node:path";

import { startServer, req } from "../test-helpers/server.js";

const KEY = "test-api-key";
const AUTH = { "X-API-Key": KEY };
const JSON_AUTH = { ...AUTH, "Content-Type": "application/json" };

const EXPECTED_CSP =
  "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' " +
  "https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data:; " +
  "connect-src 'self'; frame-ancestors 'none'";

test("no CORS headers are emitted when no origin allowlist is configured", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    const r = await req(s.url, "/api/health", { headers: { Origin: "https://attacker.example" } });
    assert.equal(r.headers.get("access-control-allow-origin"), null);
    assert.equal(r.headers.get("access-control-allow-credentials"), null);
  } finally {
    await s.stop();
  }
});

test("an explicit allowlist echoes only exact matches and never allows credentials", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY, CORS_ORIGINS: "https://ok.example,https://two.example" } });
  try {
    const good = await req(s.url, "/api/health", { headers: { Origin: "https://ok.example" } });
    assert.equal(good.headers.get("access-control-allow-origin"), "https://ok.example");
    assert.match(good.headers.get("vary") ?? "", /Origin/i);
    assert.equal(good.headers.get("access-control-allow-credentials"), null);

    const second = await req(s.url, "/api/health", { headers: { Origin: "https://two.example" } });
    assert.equal(second.headers.get("access-control-allow-origin"), "https://two.example");

    for (const origin of ["https://evil.example", "https://ok.example.evil.com", "http://ok.example"]) {
      const bad = await req(s.url, "/api/health", { headers: { Origin: origin } });
      assert.equal(bad.headers.get("access-control-allow-origin"), null, origin);
    }
  } finally {
    await s.stop();
  }
});

test("security headers are present and the framework banner is gone", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    const r = await req(s.url, "/api/health");
    assert.equal(r.headers.get("x-powered-by"), null);
    assert.equal(r.headers.get("x-content-type-options"), "nosniff");
    assert.equal(r.headers.get("x-frame-options"), "DENY");
    assert.equal(r.headers.get("referrer-policy"), "no-referrer");
    assert.equal(r.headers.get("content-security-policy"), EXPECTED_CSP);

    // The SPA itself must carry them too, not just the API.
    const page = await fetch(s.url + "/");
    assert.equal(page.headers.get("content-security-policy"), EXPECTED_CSP);
    assert.equal(page.headers.get("x-powered-by"), null);
    await page.text();
  } finally {
    await s.stop();
  }
});

test("parameter allowlists reject injected encoder values with the offending field", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY }, localMode: true, files: { "a.mp4": "x" } });
  try {
    const r = await req(s.url, "/api/transcode", {
      method: "POST",
      headers: JSON_AUTH,
      body: JSON.stringify({ uploadId: "a.mp4", encoder: "--help" }),
    });
    assert.equal(r.status, 400);
    assert.equal(r.json.field, "encoder");

    const w = await req(s.url, "/api/transcode", {
      method: "POST",
      headers: JSON_AUTH,
      body: JSON.stringify({ uploadId: "a.mp4", workers: 10000 }),
    });
    assert.equal(w.status, 400);
    assert.equal(w.json.field, "workers");
  } finally {
    await s.stop();
  }
});

test("local endpoints return 501 and create no directories when no coordinator ships", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY }, files: { "a.mp4": "x" } });
  try {
    const outputs = path.join(s.stateDir, "outputs");
    const before = await readdir(outputs);

    for (const route of ["/api/transcode", "/api/analyze"]) {
      const r = await req(s.url, route, {
        method: "POST",
        headers: JSON_AUTH,
        body: JSON.stringify({ uploadId: "a.mp4" }),
      });
      assert.equal(r.status, 501, route);
      assert.match(r.json.error, /not available/i);
    }

    const after = await readdir(outputs);
    assert.deepEqual(after, before, "501 must short-circuit before any mkdir");
  } finally {
    await s.stop();
  }
});

test("the master override is ignored outside desktop mode", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY, CLUSTER_MASTER: "configured.invalid:9900" } });
  try {
    const r = await req(s.url, "/api/cluster/status?master=127.0.0.1:1", { headers: AUTH });
    assert.equal(r.json.master, "configured.invalid:9900");
    // A body-supplied override must be ignored too.
    const p = await req(s.url, "/api/cluster/transcode", {
      method: "POST",
      headers: JSON_AUTH,
      body: JSON.stringify({ uploadId: "missing.mp4", master: "127.0.0.1:1", format: "mp4" }),
    });
    assert.notEqual(p.status, 200);
  } finally {
    await s.stop();
  }
});

test("cluster submission rejects any container other than mp4", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY }, files: { "a.mp4": "x" } });
  try {
    const r = await req(s.url, "/api/cluster/transcode", {
      method: "POST",
      headers: JSON_AUTH,
      body: JSON.stringify({ uploadId: "a.mp4", format: "hls" }),
    });
    assert.equal(r.status, 400);
    assert.equal(r.json.error, "Cluster mode produces MP4 only");
    assert.equal(r.json.field, "format");
  } finally {
    await s.stop();
  }
});

test("a cluster job with no output yet returns 404, not a 500 from a null path", async () => {
  const s = await startServer({
    env: { TRANSCODER_API_KEY: KEY, CLUSTER_MASTER: "127.0.0.1:1" },
    files: { "a.mp4": "x" },
  });
  try {
    // Submission fails (nothing listening), leaving a job whose outputDir is null.
    await req(s.url, "/api/cluster/transcode", {
      method: "POST",
      headers: JSON_AUTH,
      body: JSON.stringify({ uploadId: "a.mp4", format: "mp4" }),
    });
    const jobs = await req(s.url, "/api/jobs", { headers: AUTH });
    assert.equal(jobs.status, 200);
    assert.equal(jobs.json.length, 1, "expected the failed cluster job to be tracked");
    const id = jobs.json[0].id;

    assert.equal((await req(s.url, `/api/jobs/${id}/files`, { headers: AUTH })).status, 404);
    assert.equal((await req(s.url, `/api/download/${id}/output.mp4`, { headers: AUTH })).status, 404);
  } finally {
    await s.stop();
  }
});

// Bounded explicitly: against unfixed code this route really dials the target
// with the server's 300s socket timeout, so the test must cut it short itself.
test("url-import refuses loopback, link-local, private and non-http targets", { timeout: 60_000 }, async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    const targets = [
      "http://169.254.169.254/latest/meta-data/",
      "http://127.0.0.1:1/x",
      "http://10.0.0.1/x",
      "http://192.168.1.1/x",
      "http://[::1]:1/x",
      "file:///etc/passwd",
    ];
    for (const url of targets) {
      const r = await req(s.url, "/api/url-import", {
        method: "POST",
        headers: JSON_AUTH,
        body: JSON.stringify({ url }),
      });
      assert.equal(r.status, 400, `${url} -> ${r.status} ${r.text}`);
      assert.equal(r.json.error, "URL not allowed", url);
    }
  } finally {
    await s.stop();
  }
});

test("upstream failures return fixed text with no internal detail appended", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY, CLUSTER_MASTER: "127.0.0.1:1" } });
  try {
    const r = await req(s.url, "/api/cluster/status", { headers: AUTH });
    assert.equal(r.status, 503);
    assert.equal(r.json.error, "Cluster unreachable");

    const n = await req(s.url, "/api/cluster/nodes", { headers: AUTH });
    assert.equal(n.status, 503);
    assert.equal(n.json.error, "Cluster unreachable");
  } finally {
    await s.stop();
  }
});

test("a failed cluster submission does not surface the underlying error to clients", async () => {
  const s = await startServer({
    env: { TRANSCODER_API_KEY: KEY, CLUSTER_MASTER: "127.0.0.1:1" },
    files: { "a.mp4": "x" },
  });
  try {
    const r = await req(s.url, "/api/cluster/transcode", {
      method: "POST",
      headers: JSON_AUTH,
      body: JSON.stringify({ uploadId: "a.mp4", format: "mp4" }),
    });
    assert.equal(r.json.error, "Cluster submission failed");
    // jobSummary exposes errorMessage, so genericising the response alone is not enough.
    const jobs = await req(s.url, "/api/jobs", { headers: AUTH });
    assert.equal(jobs.json[0].errorMessage, "Cluster submission failed");
  } finally {
    await s.stop();
  }
});
