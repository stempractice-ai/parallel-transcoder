import { test } from "node:test";
import assert from "node:assert/strict";

import { timingSafeMatch } from "../lib/auth.js";

test("timingSafeMatch accepts an exact match", () => {
  assert.equal(timingSafeMatch("s3cret", "s3cret"), true);
  assert.equal(timingSafeMatch("a".repeat(64), "a".repeat(64)), true);
});

test("timingSafeMatch rejects a different value of the same length", () => {
  assert.equal(timingSafeMatch("s3cret", "s3crey"), false);
});

test("timingSafeMatch rejects differing lengths without throwing", () => {
  // crypto.timingSafeEqual throws on unequal buffer lengths; hashing first is
  // what makes this safe, so a length mismatch must return false, not raise.
  assert.equal(timingSafeMatch("short", "a-much-longer-key"), false);
  assert.equal(timingSafeMatch("a-much-longer-key", "short"), false);
});

test("timingSafeMatch rejects missing or empty credentials", () => {
  assert.equal(timingSafeMatch(undefined, "k"), false);
  assert.equal(timingSafeMatch(null, "k"), false);
  assert.equal(timingSafeMatch("", "k"), false);
  assert.equal(timingSafeMatch("k", undefined), false);
  assert.equal(timingSafeMatch("k", null), false);
  // An unset server-side key must never authenticate anyone.
  assert.equal(timingSafeMatch("", ""), false);
  assert.equal(timingSafeMatch("k", ""), false);
});

test("timingSafeMatch rejects non-string inputs", () => {
  assert.equal(timingSafeMatch(["k"], "k"), false);
  assert.equal(timingSafeMatch(42, "42"), false);
  assert.equal(timingSafeMatch({}, "[object Object]"), false);
});
