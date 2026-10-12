/**
 * Source analysis: on the cluster it goes through the master's
 * AnalyzeRequest/AnalyzeResult round trip and shares one object-store copy
 * with the transcode that follows; locally it passes the coordinator's
 * --smart-report through.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import path from "node:path";
import { writeFile, chmod } from "node:fs/promises";
import { WebSocketServer } from "ws";

import { startServer, req } from "../test-helpers/server.js";

const KEY = "test-api-key";

const MEDIA = {
  duration_secs: 6,
  width: 320,
  height: 240,
  fps: 25,
  codec: "h264",
  profile: "High",
  pix_fmt: "yuv420p",
  avg_bitrate_bps: 400000,
  has_audio: true,
};
const RECOMMENDATION = { mode: "smart-auto", preset: "medium", reasons: ["x"], copyable_gop_fraction: 0.5 };

/** S3 stand-in: accepts any request and counts PUTs. */
async function startS3() {
  const s3 = { puts: 0 };
  const server = http.createServer((r, res) => {
    if (r.method === "PUT") s3.puts++;
    r.resume();
    r.on("end", () => { res.writeHead(200, { ETag: '"stub"' }); res.end(); });
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  s3.url = `http://127.0.0.1:${server.address().port}`;
  s3.close = () => new Promise((r) => server.close(r));
  return s3;
}

/** Cluster master stand-in: answers op 36 via `analyze(d)` and accepts any JobSubmit. */
async function startMaster(analyze) {
  const master = { received: [] };
  const wss = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await new Promise((r) => wss.once("listening", r));
  wss.on("connection", (ws) => {
    ws.on("message", (data) => {
      let msg;
      try { msg = JSON.parse(data.toString()); } catch { return; }
      master.received.push(msg);
      if (msg.op === 36) ws.send(JSON.stringify(analyze(msg.d)));
      if (msg.op === 30) ws.send(JSON.stringify({ op: 31, d: { job_id: msg.d.job_id, total_segments: 1 } }));
    });
  });
  master.address = `127.0.0.1:${wss.address().port}`;
  master.close = () => new Promise((r) => {
    for (const c of wss.clients) c.terminate();
    wss.close(r);
  });
  return master;
}

async function startStack(analyze) {
  const s3 = await startS3();
  const master = await startMaster(analyze);
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
  return { s, s3, master, stop: async () => { await s.stop(); await master.close(); await s3.close(); } };
}

const post = (s, pathname, body) => req(s.url, pathname, {
  method: "POST",
  headers: { "X-API-Key": KEY, "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

test("cluster analysis uploads the source once and returns the master's recommendation", async () => {
  const { s, s3, master, stop } = await startStack((d) => ({
    op: 37,
    d: { request_id: d.request_id, media: MEDIA, recommendation: RECOMMENDATION },
  }));
  try {
    const a = await post(s, "/api/cluster/analyze", { uploadId: "src.mp4", encoder: "libx264", crf: 20 });
    assert.equal(a.status, 200, JSON.stringify(a.json));
    assert.deepEqual(a.json, { media: MEDIA, recommendation: RECOMMENDATION, source: "cluster" });

    const req36 = master.received.find((m) => m.op === 36).d;
    assert.equal(req36.source_url, "s3://transcoder-segments/sources/src.mp4");
    assert.equal(req36.encoder, "libx264");
    assert.equal(req36.crf, 20);

    const t = await post(s, "/api/cluster/transcode", { uploadId: "src.mp4", format: "mp4", mode: "smart-auto" });
    assert.equal(t.status, 200, JSON.stringify(t.json));
    const req30 = master.received.find((m) => m.op === 30).d;
    assert.equal(req30.config.mode, "smart-auto");
    assert.equal(req30.srt_input_url, req36.source_url);

    assert.equal(s3.puts, 1);
  } finally {
    await stop();
  }
});

test("a master error becomes a generic 502", async () => {
  const { s, stop } = await startStack(() => ({ op: 255, d: { code: 500, message: "Analysis failed: boom" } }));
  try {
    const r = await post(s, "/api/cluster/analyze", { uploadId: "src.mp4" });
    assert.equal(r.status, 502);
    assert.deepEqual(r.json, { error: "Cluster analysis failed" });
    assert.doesNotMatch(r.text, /boom/);
  } finally {
    await stop();
  }
});

test("local analysis passes the coordinator's recommendation through", async () => {
  const s = await startServer({ localMode: true, env: { TRANSCODER_API_KEY: KEY }, files: { "src.mp4": "x" } });
  try {
    const report = { media: MEDIA, recommendation: RECOMMENDATION, segments: [] };
    const bin = path.join(s.resourcesDir, "bin", "transcoder-coordinator");
    await writeFile(bin, `#!/bin/sh\ncat <<'EOF'\n${JSON.stringify(report)}\nEOF\n`);
    await chmod(bin, 0o755);

    const r = await post(s, "/api/analyze", { uploadId: "src.mp4" });
    assert.equal(r.status, 200, JSON.stringify(r.json));
    assert.deepEqual(r.json.recommendation, RECOMMENDATION);
  } finally {
    await s.stop();
  }
});
