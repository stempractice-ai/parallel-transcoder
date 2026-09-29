import os from "node:os";

/** Containers the coordinator can produce. */
export const FORMATS = Object.freeze(["hls", "mp4"]);

/** Segmentation strategies accepted by the coordinator CLI. */
export const MODES = Object.freeze(["normal", "copy", "smart", "smart-auto"]);

/** The encoders documented in CLAUDE.md — nothing else may reach argv. */
export const ENCODERS = Object.freeze([
  "libx264",
  "h264_videotoolbox",
  "h264_nvenc",
  "h264_vaapi",
  "libx265",
  "hevc_videotoolbox",
  "hevc_nvenc",
  "hevc_vaapi",
  "libsvtav1",
  "libaom-av1",
  "av1_nvenc",
]);

/** x264-style presets. SVT-AV1 additionally accepts "0".."12". */
export const PRESETS = Object.freeze([
  "ultrafast", "superfast", "veryfast", "faster", "fast",
  "medium", "slow", "slower", "veryslow",
]);

const SVT_PRESETS = Object.freeze(Array.from({ length: 13 }, (_, i) => String(i)));

const reject = (field, error) => ({ field, error });

function checkEnum(body, key, allowed) {
  if (body[key] === undefined) return null;
  if (typeof body[key] !== "string" || !allowed.includes(body[key])) {
    return reject(key, `${key} must be one of: ${allowed.join(", ")}`);
  }
  return null;
}

function checkInt(body, key, min, max) {
  if (body[key] === undefined) return null;
  const n = body[key];
  if (typeof n !== "number" || !Number.isInteger(n) || n < min || n > max) {
    return reject(key, `${key} must be an integer between ${min} and ${max}`);
  }
  return null;
}

/**
 * Validate a transcode/analyze request body against strict allowlists.
 *
 * Returns null when acceptable, otherwise `{ field, error }` describing the
 * first problem. Absent optional fields are valid — the routes supply their
 * own defaults.
 *
 * `maxWorkers` is injected so the ceiling is testable without depending on the
 * host's CPU count.
 */
export function validateTranscodeParams(
  body,
  { cluster = false, maxWorkers = Math.min(64, os.cpus().length) } = {},
) {
  if (!body || typeof body !== "object") return reject("body", "request body must be an object");

  if (typeof body.uploadId !== "string" || body.uploadId.length === 0) {
    return reject("uploadId", "uploadId is required");
  }

  if (cluster) {
    // The cluster master's assembly path always emits MP4 and ignores the
    // requested container, so accepting anything else would silently lie.
    if (body.format !== undefined && body.format !== "mp4") {
      return reject("format", "Cluster mode produces MP4 only");
    }
  } else {
    const e = checkEnum(body, "format", FORMATS);
    if (e) return e;
  }

  for (const [key, allowed] of [["mode", MODES], ["encoder", ENCODERS]]) {
    const e = checkEnum(body, key, allowed);
    if (e) return e;
  }

  if (body.preset !== undefined) {
    const ok = typeof body.preset === "string"
      && (PRESETS.includes(body.preset) || SVT_PRESETS.includes(body.preset));
    if (!ok) return reject("preset", "preset must be an x264 preset name or \"0\"-\"12\"");
  }

  const crf = checkInt(body, "crf", 0, 63);
  if (crf) return crf;

  const workers = checkInt(body, "workers", 0, maxWorkers);
  if (workers) return workers;

  if (body.smartTolerance !== undefined) {
    const t = body.smartTolerance;
    if (typeof t !== "number" || !Number.isFinite(t) || t < 0.05 || t > 1.0) {
      return reject("smartTolerance", "smartTolerance must be a number between 0.05 and 1.0");
    }
  }

  if (body.verbose !== undefined && typeof body.verbose !== "boolean") {
    return reject("verbose", "verbose must be a boolean");
  }

  return null;
}
