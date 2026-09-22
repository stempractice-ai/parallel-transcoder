import { test } from "node:test";
import assert from "node:assert/strict";

import { validateTranscodeParams, ENCODERS, PRESETS } from "../lib/validate.js";

const VALID = {
  uploadId: "clip_1700000000.mp4",
  format: "hls",
  mode: "smart",
  smartTolerance: 0.3,
  crf: 23,
  preset: "medium",
  encoder: "libx264",
  workers: 4,
  verbose: false,
};

test("a fully specified valid body passes", () => {
  assert.equal(validateTranscodeParams(VALID, { maxWorkers: 8 }), null);
});

test("omitted optional fields are valid — routes supply the defaults", () => {
  assert.equal(validateTranscodeParams({ uploadId: "a.mp4" }, { maxWorkers: 8 }), null);
});

test("uploadId is required and must be a non-empty string", () => {
  for (const body of [{}, { uploadId: "" }, { uploadId: 7 }, { uploadId: null }]) {
    const r = validateTranscodeParams(body, { maxWorkers: 8 });
    assert.ok(r, `expected rejection for ${JSON.stringify(body)}`);
    assert.equal(r.field, "uploadId");
  }
});

test("each out-of-allowlist value is rejected on its own field", () => {
  const cases = [
    ["format", "webm"],
    ["mode", "turbo"],
    ["encoder", "--help"],
    ["encoder", "libx264; rm -rf /"],
    ["preset", "insane"],
    ["crf", -1],
    ["crf", 64],
    ["crf", "x"],
    ["crf", 23.5],
    ["workers", 10000],
    ["workers", -1],
    ["workers", 2.5],
    ["smartTolerance", 0.01],
    ["smartTolerance", 2],
    ["smartTolerance", Number.NaN],
    ["verbose", "yes"],
  ];
  for (const [field, value] of cases) {
    const r = validateTranscodeParams({ ...VALID, [field]: value }, { maxWorkers: 64 });
    assert.ok(r, `expected rejection for ${field}=${String(value)}`);
    assert.equal(r.field, field, `wrong field reported for ${field}=${String(value)}`);
    assert.equal(typeof r.error, "string");
  }
});

test("every documented encoder is accepted", () => {
  assert.equal(ENCODERS.length, 11);
  for (const encoder of ENCODERS) {
    assert.equal(validateTranscodeParams({ ...VALID, encoder }, { maxWorkers: 8 }), null, encoder);
  }
});

test("every x264 preset and every SVT-AV1 numeric preset is accepted", () => {
  for (const preset of PRESETS) {
    assert.equal(validateTranscodeParams({ ...VALID, preset }, { maxWorkers: 8 }), null, preset);
  }
  for (let i = 0; i <= 12; i++) {
    assert.equal(validateTranscodeParams({ ...VALID, preset: String(i) }, { maxWorkers: 8 }), null, String(i));
  }
  assert.equal(validateTranscodeParams({ ...VALID, preset: "13" }, { maxWorkers: 8 })?.field, "preset");
});

test("the worker ceiling comes from maxWorkers, not the host CPU count", () => {
  assert.equal(validateTranscodeParams({ ...VALID, workers: 4 }, { maxWorkers: 4 }), null);
  const r = validateTranscodeParams({ ...VALID, workers: 5 }, { maxWorkers: 4 });
  assert.equal(r?.field, "workers");
  // 0 means auto-detect in the coordinator and must stay legal.
  assert.equal(validateTranscodeParams({ ...VALID, workers: 0 }, { maxWorkers: 4 }), null);
});

test("cluster mode accepts only mp4 and says why", () => {
  assert.equal(validateTranscodeParams({ uploadId: "a.mp4", format: "mp4" }, { cluster: true }), null);
  const r = validateTranscodeParams({ uploadId: "a.mp4", format: "hls" }, { cluster: true });
  assert.deepEqual(r, { error: "Cluster mode produces MP4 only", field: "format" });
});

test("hls stays legal outside cluster mode", () => {
  assert.equal(validateTranscodeParams({ uploadId: "a.mp4", format: "hls" }, { maxWorkers: 8 }), null);
});
