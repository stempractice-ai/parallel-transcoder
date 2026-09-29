import crypto from "node:crypto";

/**
 * Constant-time credential comparison.
 *
 * Both sides are hashed first so the buffers handed to timingSafeEqual are
 * always 32 bytes: it throws on unequal lengths, and comparing raw strings
 * would leak the expected key's length through that error path.
 *
 * An empty or non-string expected value never matches, so an unconfigured
 * server-side key cannot authenticate anyone.
 */
export function timingSafeMatch(provided, expected) {
  if (typeof provided !== "string" || typeof expected !== "string") return false;
  if (provided.length === 0 || expected.length === 0) return false;
  const a = crypto.createHash("sha256").update(provided, "utf8").digest();
  const b = crypto.createHash("sha256").update(expected, "utf8").digest();
  return crypto.timingSafeEqual(a, b);
}
