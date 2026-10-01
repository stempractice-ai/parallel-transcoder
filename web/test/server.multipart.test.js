/**
 * A malformed multipart upload must be rejected without taking the server
 * down. multer 1.4.5-lts.2 let busboy raise a second error after the first
 * had already been handled; it surfaced as an uncaught exception and the
 * process exited, losing every in-memory job (GHSA-4pg4-qvpc-4q3h).
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import { startServer, req } from "../test-helpers/server.js";

const KEY = "test-api-key";
const BOUNDARY = "AaB03x";

test("a malformed multipart upload is rejected and the server stays up", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    // A file part's headers followed straight by the closing boundary: busboy
    // reports "Malformed part header" and then "Unexpected end of form".
    const body = [
      `--${BOUNDARY}`,
      'Content-Disposition: form-data; name="video"; filename="a.mp4"',
      "Content-Type: video/mp4",
      "",
      `--${BOUNDARY}--`,
      "",
    ].join("\r\n");
    const r = await req(s.url, "/api/upload", {
      method: "POST",
      headers: { "X-API-Key": KEY, "Content-Type": `multipart/form-data; boundary=${BOUNDARY}` },
      body,
    });
    assert.equal(r.status, 400, JSON.stringify(r.json));
    assert.deepEqual(r.json, { error: "Upload error: Malformed part header" });

    // Give a late busboy error time to surface before checking liveness.
    await new Promise((resolve) => setTimeout(resolve, 500));
    assert.equal(s.child.exitCode, null, `server exited:\n${s.stderr()}`);
    const health = await req(s.url, "/api/health");
    assert.equal(health.status, 200);
  } finally {
    await s.stop();
  }
});

test("a multipart upload cut off before its closing boundary gets 400", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    const body = [
      `--${BOUNDARY}`,
      'Content-Disposition: form-data; name="video"; filename="a.mp4"',
      "Content-Type: video/mp4",
      "",
      "partial bytes",
    ].join("\r\n");
    const r = await req(s.url, "/api/upload", {
      method: "POST",
      headers: { "X-API-Key": KEY, "Content-Type": `multipart/form-data; boundary=${BOUNDARY}` },
      body,
    });
    assert.equal(r.status, 400, JSON.stringify(r.json));
    assert.match(r.json.error, /^Upload error: Unexpected end of (form|file)$/);
  } finally {
    await s.stop();
  }
});

// The upload contract the SPA and e2e rely on: the file lands byte-for-byte
// under a sanitised, timestamped name, and the response reports it.
test("a valid upload is stored under a sanitised name and reported", async () => {
  const s = await startServer({ env: { TRANSCODER_API_KEY: KEY } });
  try {
    const bytes = Buffer.from(Array.from({ length: 4096 }, (_, i) => i % 251));
    const form = new FormData();
    form.append("video", new Blob([bytes], { type: "video/mp4" }), "my clip (1).mp4");
    const r = await req(s.url, "/api/upload", { method: "POST", headers: { "X-API-Key": KEY }, body: form });
    assert.equal(r.status, 200, JSON.stringify(r.json));
    assert.match(r.json.uploadId, /^my_clip__1__\d+\.mp4$/);
    assert.equal(r.json.originalName, "my clip (1).mp4");
    assert.equal(r.json.size, bytes.length);
    assert.deepEqual(fs.readFileSync(path.join(s.stateDir, "uploads", r.json.uploadId)), bytes);
  } finally {
    await s.stop();
  }
});
