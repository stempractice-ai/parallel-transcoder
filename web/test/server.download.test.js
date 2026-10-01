/**
 * Proves the assembled cluster output is streamed to disk rather than
 * buffered, and that a failed transfer leaves nothing behind.
 *
 * Mirrors server.upload.test.js: the web pod is limited to 256Mi, so a
 * buffered read of a large output OOM-kills it and loses all in-memory job
 * state. A fake cluster master reports the job complete with an s3:// output,
 * and a local S3 stand-in serves that object.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { WebSocketServer } from "ws";

import { startServer, req } from "../test-helpers/server.js";

const execFileAsync = promisify(execFile);
const KEY = "test-api-key";
const SIZE = 384 * 1024 * 1024; // 384 MiB
// 256 MiB is the web pod's actual memory limit (web-frontend.yaml
// `limits.memory`), the same threshold the upload test uses.
const RSS_CEILING_BYTES = 256 * 1024 * 1024;
const CHUNK = Buffer.alloc(8 * 1024 * 1024, 0x61);

/**
 * S3 stand-in: accepts any PUT; answers every GET with `size` bytes, written
 * with backpressure so the stand-in never holds the object. With
 * `truncateAt`, the GET stops after that many bytes and drops the socket.
 */
async function startS3({ size, truncateAt = null }) {
  const server = http.createServer((r, res) => {
    if (r.method !== "GET") {
      r.resume();
      r.on("end", () => { res.writeHead(200, { ETag: '"stub"' }); res.end(); });
      return;
    }
    res.writeHead(200, { "Content-Type": "application/octet-stream", "Content-Length": size });
    const limit = truncateAt ?? size;
    let sent = 0;
    const pump = () => {
      while (sent < limit) {
        const n = Math.min(CHUNK.length, limit - sent);
        sent += n;
        if (!res.write(n === CHUNK.length ? CHUNK : CHUNK.subarray(0, n))) {
          res.once("drain", pump);
          return;
        }
      }
      if (truncateAt === null) res.end();
      else res.destroy();
    };
    pump();
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return { url: `http://127.0.0.1:${server.address().port}`, close: () => new Promise((r) => server.close(r)) };
}

/** Cluster master stand-in: accepts any JobSubmit and completes it at once. */
async function startMaster() {
  const wss = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await new Promise((r) => wss.once("listening", r));
  wss.on("connection", (ws) => {
    ws.on("message", (data) => {
      let msg;
      try { msg = JSON.parse(data.toString()); } catch { return; }
      if (msg.op !== 30) return;
      const jobId = msg.d.job_id;
      ws.send(JSON.stringify({ op: 31, d: { job_id: jobId, total_segments: 1 } }));
      ws.send(JSON.stringify({
        op: 33,
        d: { job_id: jobId, total_segments: 1, output_uri: `s3://transcoder-segments/jobs/${jobId}/final/output.mp4` },
      }));
    });
  });
  return { address: `127.0.0.1:${wss.address().port}`, close: () => new Promise((r) => wss.close(r)) };
}

/** Peak RSS of a pid, sampled until `stop()` is called. */
function sampleRss(pid) {
  let peak = 0;
  let running = true;
  let unsupported = false;
  const loop = (async () => {
    while (running) {
      try {
        const { stdout } = await execFileAsync("ps", ["-o", "rss=", "-p", String(pid)]);
        const kb = parseInt(stdout.trim(), 10);
        if (Number.isFinite(kb)) peak = Math.max(peak, kb * 1024);
      } catch {
        unsupported = true;
        return;
      }
      await new Promise((r) => setTimeout(r, 50));
    }
  })();
  return { stop: async () => { running = false; await loop; return { peak, unsupported }; } };
}

async function startStack(s3Opts) {
  const s3 = await startS3(s3Opts);
  const master = await startMaster();
  const s = await startServer({
    env: {
      TRANSCODER_API_KEY: KEY,
      CLUSTER_MASTER: master.address,
      OBJECT_STORE_URL: s3.url,
      OBJECT_STORE_BUCKET: "transcoder-segments",
      AWS_ACCESS_KEY_ID: "test",
      AWS_SECRET_ACCESS_KEY: "test",
      AWS_REGION: "us-east-1",
    },
    files: { "src.mp4": "x" },
  });
  return { s, stop: async () => { await s.stop(); await master.close(); await s3.close(); } };
}

async function submit(s) {
  const r = await req(s.url, "/api/cluster/transcode", {
    method: "POST",
    headers: { "X-API-Key": KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ uploadId: "src.mp4", format: "mp4" }),
  });
  assert.equal(r.status, 200, `submit failed: ${JSON.stringify(r.json)}`);
  return r.json.jobId;
}

async function until(fn, timeoutMs, what) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 250));
  }
}

test("the assembled cluster output download streams: whole file, bounded memory", { timeout: 240_000 }, async (t) => {
  const { s, stop } = await startStack({ size: SIZE });
  try {
    const rss = sampleRss(s.child.pid);
    const jobId = await submit(s);
    await until(async () => {
      const r = await req(s.url, `/api/jobs/${jobId}/files`, { headers: { "X-API-Key": KEY } });
      return r.status === 200 && r.json.some((f) => f.name === "output.mp4" && f.size === SIZE);
    }, 180_000, "output.mp4 of the full size on the web pod");
    const { peak, unsupported } = await rss.stop();

    if (unsupported) {
      t.diagnostic("RSS ceiling skipped: `ps -o rss=` unavailable on this host");
    } else {
      assert.ok(peak > 0, "RSS sampling produced no data");
      assert.ok(
        peak < RSS_CEILING_BYTES,
        `peak RSS ${(peak / 1048576).toFixed(0)} MiB exceeded the ${RSS_CEILING_BYTES / 1048576} MiB ceiling ` +
        `while downloading ${SIZE / 1048576} MiB — the output is being buffered, not streamed`,
      );
      t.diagnostic(`peak RSS ${(peak / 1048576).toFixed(0)} MiB downloading ${SIZE / 1048576} MiB`);
    }
  } finally {
    await stop();
  }
});

test("a truncated output download leaves no file behind", { timeout: 60_000 }, async () => {
  const { s, stop } = await startStack({ size: 8 * 1024 * 1024, truncateAt: 1024 * 1024 });
  try {
    const jobId = await submit(s);
    await until(() => s.stderr().includes(`Failed to fetch assembled output for job ${jobId}`), 30_000, "the fetch failure log line");
    const r = await req(s.url, `/api/jobs/${jobId}/files`, { headers: { "X-API-Key": KEY } });
    assert.equal(r.status, 404, "a failed fetch must not expose output files");
    assert.deepEqual(fs.readdirSync(path.join(s.stateDir, "outputs", jobId)), [], "no output.mp4 or .part may remain");
  } finally {
    await stop();
  }
});
