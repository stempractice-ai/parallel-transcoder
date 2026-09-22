/**
 * Proves the cluster-mode source upload is streamed rather than buffered.
 *
 * The wire assertions alone cannot detect the defect: a Buffer body also
 * produces a correct Content-Length with no chunked encoding. The memory
 * ceiling is what distinguishes the two, and it mirrors the production failure
 * exactly — the web pod is limited to 256Mi, so a buffered read of a large
 * source OOM-kills it and loses all in-memory job state.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { open } from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

import { startServer, req } from "../test-helpers/server.js";

const execFileAsync = promisify(execFile);
const KEY = "test-api-key";
const SIZE = 384 * 1024 * 1024; // 384 MiB
// 256 MiB is the web pod's actual memory limit (web-frontend.yaml `limits.memory`),
// so this is the threshold that matters rather than an arbitrary one. Measured
// on this 384 MiB source: 479 MiB buffered (pre-fix) vs 166 MiB streamed — the
// ceiling separates them by ~1.9x above and ~1.5x below, leaving room for
// slower CI runners and GC timing.
const RSS_CEILING_BYTES = 256 * 1024 * 1024;

/** Sink that accepts any PUT, records its headers, and counts body bytes. */
async function startS3Sink() {
  const puts = [];
  const server = http.createServer((r, res) => {
    let bytes = 0;
    r.on("data", (c) => { bytes += c.length; });
    r.on("end", () => {
      puts.push({ method: r.method, url: r.url, headers: r.headers, bytes });
      res.writeHead(200, { ETag: '"stub"' });
      res.end();
    });
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return { url: `http://127.0.0.1:${server.address().port}`, puts, close: () => new Promise((r) => server.close(r)) };
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

/** Write `size` bytes without ever holding them all in memory. */
async function writeLargeFile(file, size) {
  const chunk = Buffer.alloc(8 * 1024 * 1024, 0x61);
  const fh = await open(file, "w");
  try {
    for (let written = 0; written < size; written += chunk.length) {
      await fh.write(chunk, 0, Math.min(chunk.length, size - written));
    }
  } finally {
    await fh.close();
  }
}

test("the cluster source upload streams: correct length, no aws-chunked, bounded memory", { timeout: 240_000 }, async (t) => {
  const sink = await startS3Sink();
  const s = await startServer({
    env: {
      TRANSCODER_API_KEY: KEY,
      CLUSTER_MASTER: "127.0.0.1:1", // submission fails after the upload; we only assert on the upload
      OBJECT_STORE_URL: sink.url,
      OBJECT_STORE_BUCKET: "transcoder-segments",
      AWS_ACCESS_KEY_ID: "test",
      AWS_SECRET_ACCESS_KEY: "test",
      AWS_REGION: "us-east-1",
    },
  });

  try {
    await writeLargeFile(path.join(s.stateDir, "uploads", "big.mp4"), SIZE);

    const rss = sampleRss(s.child.pid);
    await req(s.url, "/api/cluster/transcode", {
      method: "POST",
      headers: { "X-API-Key": KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ uploadId: "big.mp4", format: "mp4" }),
      timeoutMs: 180_000, // 384 MiB has to cross a loopback socket
    });
    const { peak, unsupported } = await rss.stop();

    const put = sink.puts.find((p) => p.method === "PUT");
    assert.ok(put, `no PUT reached the object store (saw ${JSON.stringify(sink.puts.map((p) => p.method))})`);
    assert.equal(Number(put.headers["content-length"]), SIZE, "Content-Length must be the real file size");
    assert.doesNotMatch(
      String(put.headers["content-encoding"] ?? ""),
      /aws-chunked/,
      "aws-chunked is rejected by OCI's S3-compatible endpoint",
    );
    assert.equal(put.bytes, SIZE, "the whole object must arrive");

    if (unsupported) {
      t.diagnostic("RSS ceiling skipped: `ps -o rss=` unavailable on this host");
    } else {
      assert.ok(peak > 0, "RSS sampling produced no data");
      assert.ok(
        peak < RSS_CEILING_BYTES,
        `peak RSS ${(peak / 1048576).toFixed(0)} MiB exceeded the ${RSS_CEILING_BYTES / 1048576} MiB ceiling ` +
        `while uploading ${SIZE / 1048576} MiB — the source is being buffered, not streamed`,
      );
      t.diagnostic(`peak RSS ${(peak / 1048576).toFixed(0)} MiB uploading ${SIZE / 1048576} MiB`);
    }
  } finally {
    await s.stop();
    await sink.close();
  }
});
