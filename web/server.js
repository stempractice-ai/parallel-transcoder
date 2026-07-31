import express from "express";
import multer from "multer";
import { WebSocketServer } from "ws";
import WebSocket from "ws";
import { spawn } from "child_process";
import crypto from "crypto";
import path from "path";
import fs from "fs";
import fsp from "fs/promises";
import { fileURLToPath } from "url";
import http from "http";
import https from "https";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RESOURCES_DIR = process.env.TRANSCODER_RESOURCES_DIR || path.join(__dirname, "..");
const COORDINATOR_BIN = path.join(RESOURCES_DIR, "bin", "transcoder-coordinator");
const LIB_DIR = path.join(RESOURCES_DIR, "lib") + path.sep;
const UPLOAD_DIR = path.join(__dirname, "uploads");
const OUTPUT_DIR = path.join(__dirname, "outputs");

const DESKTOP_MODE = process.env.DESKTOP_MODE === "1";
const PORT = DESKTOP_MODE ? 0 : parseInt(process.env.PORT || "3000", 10);
const HOST = DESKTOP_MODE ? "127.0.0.1" : undefined;
const DEFAULT_CLUSTER_MASTER = process.env.CLUSTER_MASTER || "localhost:9900";
const MAX_LOG_LINES = 200;

// Object storage (K8s mode only) — same bucket the cluster nodes use for
// segment transfer. When configured, cluster-mode submissions upload the
// raw source video here so the master can download, cut real segments,
// and re-upload them; without this the master has no way to reach a file
// that only exists on the web pod's local disk.
const OBJECT_STORE_URL = process.env.OBJECT_STORE_URL || null;
const OBJECT_STORE_BUCKET = process.env.OBJECT_STORE_BUCKET || "transcoder-segments";
const s3Client = OBJECT_STORE_URL
  ? new S3Client({
      endpoint: OBJECT_STORE_URL,
      region: process.env.AWS_REGION || "us-east-1",
      forcePathStyle: true,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      },
    })
  : null;

/** Upload the local source file to the object store; returns an s3:// URI. */
async function uploadSourceToObjectStore(jobId, inputPath) {
  const key = `jobs/${jobId}/source/${path.basename(inputPath)}`;
  // A stream Body makes the SDK default to aws-chunked transfer encoding,
  // which OCI's S3-compat endpoint rejects ("AWS chunked encoding not
  // supported"). Buffering avoids that path entirely — fine at these
  // (tens-of-MB) upload sizes.
  const body = await fsp.readFile(inputPath);
  await s3Client.send(new PutObjectCommand({
    Bucket: OBJECT_STORE_BUCKET,
    Key: key,
    Body: body,
  }));
  return `s3://${OBJECT_STORE_BUCKET}/${key}`;
}

/** Resolve cluster master from request (query/body), falling back to env default. */
function resolveMaster(req) {
  const raw = (req.query && req.query.master) || (req.body && req.body.master) || DEFAULT_CLUSTER_MASTER;
  const m = String(raw).trim().replace(/^ws:\/\//i, "").replace(/\/+$/, "");
  if (!/^[A-Za-z0-9_.\-]+:\d+$/.test(m)) {
    const err = new Error(`Invalid cluster master address: ${raw}`);
    err.statusCode = 400;
    throw err;
  }
  return m;
}

// Platform-aware library path variable
const LIB_PATH_KEY = process.platform === "darwin" ? "DYLD_LIBRARY_PATH" : "LD_LIBRARY_PATH";
const UPLOAD_LIMIT = 10 * 1024 * 1024 * 1024; // 10 GB
const PID_FILE = path.join(__dirname, ".web.pid");

// ---------------------------------------------------------------------------
// Ensure directories
// ---------------------------------------------------------------------------
fs.mkdirSync(UPLOAD_DIR, { recursive: true });
fs.mkdirSync(OUTPUT_DIR, { recursive: true });

// ---------------------------------------------------------------------------
// Job Manager
// ---------------------------------------------------------------------------

/**
 * @typedef {Object} JobState
 * @property {string}              id
 * @property {"queued"|"running"|"complete"|"error"|"cancelled"} status
 * @property {import("child_process").ChildProcess|null} process
 * @property {object}              config
 * @property {string}              outputDir
 * @property {string[]}            logs
 * @property {string}              phase
 * @property {number}              percent
 * @property {string|null}         errorMessage
 * @property {Date}                createdAt
 */

/** @type {Map<string, JobState>} */
const jobs = new Map();

/** @type {Map<string, Set<import("ws").WebSocket>>} */
const subscribers = new Map();

// ---------------------------------------------------------------------------
// Express app
// ---------------------------------------------------------------------------
const app = express();
app.use(express.json());

// CORS — allow any origin for API access
if (!DESKTOP_MODE) {
  app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", req.headers.origin || "*");
    res.header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
    res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key");
    res.header("Access-Control-Allow-Credentials", "true");
    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
  });
}

// Optional API key authentication (set TRANSCODER_API_KEY env var to enable)
const API_KEY = process.env.TRANSCODER_API_KEY || null;
app.use("/api", (req, res, next) => {
  if (!API_KEY) return next(); // No key configured — open access
  const provided = req.headers["x-api-key"] || req.query.api_key;
  if (provided === API_KEY) return next();
  res.status(401).json({ error: "Invalid or missing API key" });
});

app.use(express.static(path.join(__dirname, "public")));

// Multer for uploads
const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, UPLOAD_DIR),
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname);
    const base = path.basename(file.originalname, ext).replace(/[^a-zA-Z0-9_-]/g, "_");
    cb(null, `${base}_${Date.now()}${ext}`);
  },
});
const upload = multer({ storage, limits: { fileSize: UPLOAD_LIMIT } });

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Health / server info
app.get("/api/health", (_req, res) => {
  res.json({
    status: "ok",
    version: "1.0.0",
    uptime: Math.floor(process.uptime()),
    platform: process.platform,
    arch: process.arch,
    jobs: {
      total: jobs.size,
      running: [...jobs.values()].filter(j => j.status === "running").length,
    },
  });
});

// Upload video
app.post("/api/upload", upload.single("video"), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No file uploaded" });
  }
  res.json({
    uploadId: req.file.filename,
    originalName: req.file.originalname,
    size: req.file.size,
  });
});

// Import video from URL
app.post("/api/url-import", async (req, res) => {
  const { url } = req.body;
  if (!url || typeof url !== "string") {
    return res.status(400).json({ error: "url is required" });
  }

  // Validate URL scheme
  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    return res.status(400).json({ error: "Invalid URL" });
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return res.status(400).json({ error: "Only http and https URLs are supported" });
  }

  // Derive filename from URL path or use a generic name
  const urlPath = parsed.pathname.split("/").pop() || "video";
  const safeName = urlPath.replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 200);
  const ext = path.extname(safeName) || ".mp4";
  const base = path.basename(safeName, ext);
  const filename = `${base}_${Date.now()}${ext}`;
  const destPath = path.join(UPLOAD_DIR, filename);

  try {
    await downloadFile(url, destPath);
    const stat = fs.statSync(destPath);
    res.json({
      uploadId: filename,
      originalName: safeName,
      size: stat.size,
      source: "url",
    });
  } catch (err) {
    // Clean up partial download
    try { fs.unlinkSync(destPath); } catch {}
    res.status(500).json({ error: `Download failed: ${err.message}` });
  }
});

// Start transcode job
app.post("/api/transcode", async (req, res) => {
  const {
    uploadId,
    format = "hls",
    mode = "normal",
    smartTolerance = 0.3,
    crf = 23,
    preset = "medium",
    encoder = "libx264",
    workers = 0,
    verbose = false,
  } = req.body;

  if (!uploadId) {
    return res.status(400).json({ error: "uploadId is required" });
  }

  const inputPath = path.join(UPLOAD_DIR, path.basename(uploadId));
  if (!fs.existsSync(inputPath)) {
    return res.status(404).json({ error: "Uploaded file not found" });
  }

  const jobId = crypto.randomUUID();
  const jobOutputDir = path.join(OUTPUT_DIR, jobId);
  fs.mkdirSync(jobOutputDir, { recursive: true });

  // Build coordinator args
  const args = [
    "--input", inputPath,
    "--output", jobOutputDir,
    "--format", format,
    "--workers", String(workers),
  ];

  switch (mode) {
    case "copy":
      args.push("--copy");
      break;
    case "smart":
      args.push("--smart", "--smart-tolerance", String(smartTolerance));
      break;
    case "smart-auto":
      args.push("--smart-auto", "--smart-tolerance", String(smartTolerance));
      break;
    case "normal":
    default:
      args.push("--fast");
      break;
  }

  if (mode !== "copy") {
    args.push("--crf", String(crf), "--preset", preset, "--encoder", encoder);
  }

  if (verbose) {
    args.push("--verbose");
  }

  /** @type {JobState} */
  const job = {
    id: jobId,
    status: "running",
    process: null,
    config: { uploadId, format, mode, crf, preset, encoder, workers, verbose, smartTolerance },
    outputDir: jobOutputDir,
    logs: [],
    phase: "starting",
    percent: 0,
    errorMessage: null,
    createdAt: new Date(),
    startedAt: new Date(),
    segmentsCompleted: 0,
    segmentsTotal: 0,
  };
  jobs.set(jobId, job);

  const child = spawn(COORDINATOR_BIN, args, {
    env: { ...process.env, [LIB_PATH_KEY]: LIB_DIR },
    stdio: ["ignore", "pipe", "pipe"],
  });
  job.process = child;

  child.stderr.on("data", (data) => {
    const text = data.toString();
    for (const line of text.split("\n")) {
      if (!line.trim()) continue;
      pushLog(jobId, line);
      parsePhase(jobId, line);
    }
  });

  child.stdout.on("data", (data) => {
    // stdout is generally not used for progress, but capture it
    const text = data.toString();
    for (const line of text.split("\n")) {
      if (line.trim()) pushLog(jobId, line);
    }
  });

  child.on("close", (code) => {
    job.process = null;
    if (job.status === "cancelled") return; // already handled

    if (code === 0) {
      job.status = "complete";
      job.phase = "complete";
      job.percent = 100;
      const outputFiles = listOutputFiles(jobOutputDir);
      broadcast(jobId, { type: "complete", jobId, outputFiles });
    } else {
      job.status = "error";
      job.phase = "error";
      job.errorMessage = `Coordinator exited with code ${code}`;
      broadcast(jobId, { type: "error", jobId, message: job.errorMessage });
    }
  });

  child.on("error", (err) => {
    job.process = null;
    job.status = "error";
    job.phase = "error";
    job.errorMessage = `Failed to start coordinator: ${err.message}`;
    broadcast(jobId, { type: "error", jobId, message: job.errorMessage });
  });

  res.json({ jobId, status: "running" });
});

// Analyze (smart report)
app.post("/api/analyze", async (req, res) => {
  const {
    uploadId,
    crf = 23,
    encoder = "libx264",
    smartTolerance = 0.3,
  } = req.body;

  if (!uploadId) {
    return res.status(400).json({ error: "uploadId is required" });
  }

  const inputPath = path.join(UPLOAD_DIR, path.basename(uploadId));
  if (!fs.existsSync(inputPath)) {
    return res.status(404).json({ error: "Uploaded file not found" });
  }

  const tmpOutputDir = path.join(OUTPUT_DIR, `analyze_${Date.now()}`);
  fs.mkdirSync(tmpOutputDir, { recursive: true });

  const args = [
    "--input", inputPath,
    "--output", tmpOutputDir,
    "--smart-report",
    "--smart-tolerance", String(smartTolerance),
    "--crf", String(crf),
    "--encoder", encoder,
  ];

  try {
    const result = await runCoordinatorSync(args);
    // Clean up temp directory
    fsp.rm(tmpOutputDir, { recursive: true, force: true }).catch(() => {});
    // stdout should contain the JSON report
    try {
      const report = JSON.parse(result.stdout);
      res.json(report);
    } catch {
      res.json({ raw: result.stdout, stderr: result.stderr });
    }
  } catch (err) {
    fsp.rm(tmpOutputDir, { recursive: true, force: true }).catch(() => {});
    res.status(500).json({ error: err.message });
  }
});

// List all jobs
app.get("/api/jobs", (_req, res) => {
  const list = [];
  for (const job of jobs.values()) {
    list.push(jobSummary(job));
  }
  res.json(list);
});

// Single job
app.get("/api/jobs/:id", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: "Job not found" });
  res.json(jobSummary(job));
});

// Job output files
app.get("/api/jobs/:id/files", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: "Job not found" });
  const files = listOutputFiles(job.outputDir);
  res.json(files);
});

// Job logs
app.get("/api/jobs/:id/logs", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: "Job not found" });
  const offset = parseInt(req.query.offset || "0", 10);
  const limit = parseInt(req.query.limit || "100", 10);
  const lines = job.logs.slice(offset, offset + limit);
  res.json({ total: job.logs.length, offset, lines });
});

// Cancel / remove job
app.delete("/api/jobs/:id", async (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: "Job not found" });

  // Kill child process if still running
  if (job.process) {
    job.status = "cancelled";
    job.phase = "cancelled";
    job.process.kill("SIGTERM");
    job.process = null;
    broadcast(job.id, { type: "error", jobId: job.id, message: "Job cancelled" });
  }

  // Clean up output files
  fsp.rm(job.outputDir, { recursive: true, force: true }).catch(() => {});

  jobs.delete(req.params.id);
  subscribers.delete(req.params.id);
  res.json({ deleted: true });
});

// Download output file
app.get("/api/download/:jobId/:filename", (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: "Job not found" });

  // Path join safety: ensure filename doesn't escape the output dir
  const filename = path.basename(req.params.filename);
  const filePath = path.join(job.outputDir, filename);

  if (!filePath.startsWith(job.outputDir)) {
    return res.status(403).json({ error: "Invalid filename" });
  }

  if (!fs.existsSync(filePath)) {
    return res.status(404).json({ error: "File not found" });
  }

  res.download(filePath, filename);
});

// ---------------------------------------------------------------------------
// Cluster management endpoints
// ---------------------------------------------------------------------------

// Cluster status
app.get("/api/cluster/status", async (req, res) => {
  let master;
  try { master = resolveMaster(req); }
  catch (err) { return res.status(err.statusCode || 400).json({ error: err.message }); }
  try {
    const status = await queryCluster(master);
    res.json({ master, ...status });
  } catch (err) {
    res.status(503).json({ master, error: `Cluster unreachable: ${err.message}` });
  }
});

// List cluster nodes
app.get("/api/cluster/nodes", async (req, res) => {
  let master;
  try { master = resolveMaster(req); }
  catch (err) { return res.status(err.statusCode || 400).json({ error: err.message }); }
  try {
    const status = await queryCluster(master);
    res.json(status.nodes || []);
  } catch (err) {
    res.status(503).json({ error: `Cluster unreachable: ${err.message}` });
  }
});

// Submit transcode job to the cluster
app.post("/api/cluster/transcode", async (req, res) => {
  let master;
  try { master = resolveMaster(req); }
  catch (err) { return res.status(err.statusCode || 400).json({ error: err.message }); }

  const {
    uploadId,
    format = "hls",
    crf = 23,
    preset = "medium",
    encoder = "libx264",
  } = req.body;

  if (!uploadId) {
    return res.status(400).json({ error: "uploadId is required" });
  }

  const inputPath = path.join(UPLOAD_DIR, path.basename(uploadId));
  if (!fs.existsSync(inputPath)) {
    return res.status(404).json({ error: "Uploaded file not found" });
  }

  const jobId = crypto.randomUUID();

  try {
    let srtInputUrl = null;
    if (s3Client) {
      srtInputUrl = await uploadSourceToObjectStore(jobId, inputPath);
    }

    const result = await submitClusterJob(master, {
      jobId,
      inputPath,
      format,
      crf,
      preset,
      encoder,
      srtInputUrl,
    });
    res.json({ jobId, master, status: "submitted", clusterId: result.jobId });
  } catch (err) {
    res.status(500).json({ error: `Cluster submission failed: ${err.message}` });
  }
});

// Global error handler
app.use((err, _req, res, _next) => {
  if (err instanceof multer.MulterError) {
    return res.status(400).json({ error: `Upload error: ${err.message}` });
  }
  console.error("Unhandled error:", err);
  res.status(500).json({ error: "Internal server error" });
});

// ---------------------------------------------------------------------------
// HTTP + WebSocket server
// ---------------------------------------------------------------------------
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (request, socket, head) => {
  const { pathname } = new URL(request.url, `http://${request.headers.host}`);
  if (pathname === "/ws") {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit("connection", ws, request);
    });
  } else {
    socket.destroy();
  }
});

wss.on("connection", (ws) => {
  ws.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === "subscribe" && msg.jobId) {
        let subs = subscribers.get(msg.jobId);
        if (!subs) {
          subs = new Set();
          subscribers.set(msg.jobId, subs);
        }
        subs.add(ws);

        // Send current state to the newly subscribed client
        const job = jobs.get(msg.jobId);
        if (job) {
          safeSend(ws, {
            type: "progress",
            jobId: msg.jobId,
            phase: job.phase,
            percent: job.percent,
            message: `Subscribed — current phase: ${job.phase}`,
            startedAt: job.startedAt,
            now: Date.now(),
            segmentsCompleted: job.segmentsCompleted,
            segmentsTotal: job.segmentsTotal,
          });
          // Send recent logs
          for (const line of job.logs) {
            safeSend(ws, { type: "log", jobId: msg.jobId, line });
          }
        }
      }
    } catch {
      // ignore malformed messages
    }
  });

  ws.on("close", () => {
    // Remove from all subscriber sets
    for (const subs of subscribers.values()) {
      subs.delete(ws);
    }
  });
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Push a log line to job history and broadcast it. */
function pushLog(jobId, line) {
  const job = jobs.get(jobId);
  if (!job) return;
  job.logs.push(line);
  if (job.logs.length > MAX_LOG_LINES) {
    job.logs.shift();
  }
  broadcast(jobId, { type: "log", jobId, line });
}

/**
 * Parse coordinator stderr lines to extract phase information.
 * The coordinator uses the `tracing` crate, so lines look like:
 *   2024-01-15T10:30:00.000Z  INFO coordinator: Analyzing video ...
 */
function parsePhase(jobId, line) {
  const job = jobs.get(jobId);
  if (!job) return;

  const lower = line.toLowerCase();

  const createdMatch = line.match(/Created\s+(\d+)\s+segments/i);
  if (createdMatch) {
    const total = parseInt(createdMatch[1], 10);
    if (total > 0) job.segmentsTotal = total;
  }

  const doneMatch = line.match(/Segment\s+\d+\s+done\b/i);
  if (doneMatch) {
    job.segmentsCompleted = (job.segmentsCompleted || 0) + 1;
    if (job.segmentsTotal && job.segmentsCompleted > job.segmentsTotal) {
      job.segmentsCompleted = job.segmentsTotal;
    }
  }

  if (lower.includes("analyzing video") || lower.includes("analyzing")) {
    job.phase = "analyzing";
    job.percent = 10;
  } else if (lower.includes("creating segments") || lower.includes("segments")) {
    // Check for segment count patterns like "Created N segments"
    const match = line.match(/(\d+)\s+segments/i);
    if (match) {
      job.phase = "splitting";
      job.percent = 25;
    }
  } else if (lower.includes("pre-split") || lower.includes("presplit_") || lower.includes("splitting")) {
    job.phase = "splitting";
    job.percent = 30;
    // Try to extract progress from bar patterns like "3/10 segments"
    const match = line.match(/(\d+)\/(\d+)\s+segments/);
    if (match) {
      const done = parseInt(match[1], 10);
      const total = parseInt(match[2], 10);
      if (total > 0) {
        job.percent = 25 + Math.round((done / total) * 15); // 25-40%
      }
    }
  } else if (lower.includes("segment_") || lower.includes("transcoding") || lower.includes("encoding")) {
    job.phase = "encoding";
    // Try to extract progress
    const match = line.match(/(\d+)\/(\d+)\s+segments/);
    if (match) {
      const done = parseInt(match[1], 10);
      const total = parseInt(match[2], 10);
      if (total > 0) {
        job.percent = 40 + Math.round((done / total) * 45); // 40-85%
      }
    } else if (job.segmentsTotal > 0) {
      const ratio = Math.min(1, job.segmentsCompleted / job.segmentsTotal);
      const computed = 40 + Math.round(ratio * 45);
      if (computed > job.percent) job.percent = computed;
    } else if (job.percent < 40) {
      job.percent = 40;
    }
  } else if (lower.includes("playlist") || lower.includes("concatenat") || lower.includes("assembling")) {
    job.phase = "finalizing";
    job.percent = 90;
  } else if (lower.includes("pipeline complete")) {
    job.phase = "complete";
    job.percent = 100;
  }

  broadcast(jobId, {
    type: "progress",
    jobId,
    phase: job.phase,
    percent: job.percent,
    message: line.trim(),
    startedAt: job.startedAt,
    now: Date.now(),
    segmentsCompleted: job.segmentsCompleted,
    segmentsTotal: job.segmentsTotal,
  });
}

/** Broadcast a message to all subscribers of a job. */
function broadcast(jobId, message) {
  const subs = subscribers.get(jobId);
  if (!subs) return;
  const payload = JSON.stringify(message);
  for (const ws of subs) {
    if (ws.readyState === ws.OPEN) {
      ws.send(payload);
    }
  }
}

/** Send a message to a single WebSocket, swallowing errors. */
function safeSend(ws, message) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

/** Build a job summary (no process handle or internal state). */
function jobSummary(job) {
  return {
    id: job.id,
    status: job.status,
    phase: job.phase,
    percent: job.percent,
    config: job.config,
    errorMessage: job.errorMessage,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    segmentsCompleted: job.segmentsCompleted || 0,
    segmentsTotal: job.segmentsTotal || 0,
    logCount: job.logs.length,
  };
}

/** List files in a job's output directory. */
function listOutputFiles(dir) {
  try {
    return fs.readdirSync(dir).map((name) => {
      const stat = fs.statSync(path.join(dir, name));
      return { name, size: stat.size };
    });
  } catch {
    return [];
  }
}

/** Query the cluster master via WebSocket and return the response. */
function queryCluster(master, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://${master}`);

    const timer = setTimeout(() => {
      ws.close();
      reject(new Error("Cluster query timed out"));
    }, timeout);

    ws.on("open", () => {
      // Send a StatusRequest message (OpCode 50 = StatusRequest)
      ws.send(JSON.stringify({ op: 50, d: {} }));
    });

    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.op === 51) {  // OpCode 51 = StatusResponse
          clearTimeout(timer);
          ws.close();
          resolve(msg.d);
        }
      } catch {
        // ignore malformed messages
      }
    });

    ws.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });

    ws.on("close", () => {
      clearTimeout(timer);
    });
  });
}

/** Submit a transcode job to the cluster master. */
function submitClusterJob(master, { jobId, inputPath, format, crf, preset, encoder, srtInputUrl }) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://${master}`);

    // The master now downloads the source, cuts real segments with ffmpeg,
    // and uploads each one before replying — generous enough for a
    // multi-minute video on a single CPU core.
    const timer = setTimeout(() => {
      ws.close();
      reject(new Error("Cluster job submission timed out"));
    }, 120000);

    ws.on("open", () => {
      const fileSize = fs.statSync(inputPath).size;
      // Send JobSubmit (OpCode 30)
      ws.send(JSON.stringify({
        op: 30,
        d: {
          job_id: jobId,
          input_filename: inputPath,
          input_size_bytes: fileSize,
          config: {
            crf: Number(crf),
            preset,
            encoder,
            format,
            fast_mode: true,
            hw_decode: false,
          },
          srt_input_url: srtInputUrl || null,
        },
      }));
    });

    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.op === 31) {  // OpCode 31 = JobAccepted
          clearTimeout(timer);
          // Keep connection open for progress monitoring
          // but return immediately to the caller
          resolve({ jobId: msg.d.job_id, totalSegments: msg.d.total_segments });
        } else if (msg.op === 255) {  // OpCode 255 = Error
          clearTimeout(timer);
          ws.close();
          reject(new Error(msg.d.message || "Cluster error"));
        }
      } catch {
        // ignore malformed messages
      }
    });

    ws.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

/** Download a file from a URL, following redirects (up to 5). */
function downloadFile(url, destPath, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const doRequest = (currentUrl, redirectsLeft) => {
      const mod = currentUrl.startsWith("https") ? https : http;
      const req = mod.get(currentUrl, { headers: { "User-Agent": "ParallelTranscoder/1.0" } }, (res) => {
        // Follow redirects
        if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
          if (redirectsLeft <= 0) {
            reject(new Error("Too many redirects"));
            return;
          }
          const next = new URL(res.headers.location, currentUrl).href;
          doRequest(next, redirectsLeft - 1);
          return;
        }

        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }

        const file = fs.createWriteStream(destPath);
        res.pipe(file);
        file.on("finish", () => { file.close(resolve); });
        file.on("error", (err) => {
          try { fs.unlinkSync(destPath); } catch {}
          reject(err);
        });
      });
      req.on("error", reject);
      req.setTimeout(300000, () => {
        req.destroy();
        reject(new Error("Download timed out"));
      });
    };
    doRequest(url, maxRedirects);
  });
}

/** Run the coordinator synchronously and return stdout/stderr. */
function runCoordinatorSync(args) {
  return new Promise((resolve, reject) => {
    const child = spawn(COORDINATOR_BIN, args, {
      env: { ...process.env, [LIB_PATH_KEY]: LIB_DIR },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (d) => { stdout += d.toString(); });
    child.stderr.on("data", (d) => { stderr += d.toString(); });

    child.on("close", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        const err = new Error(`Coordinator exited with code ${code}`);
        err.stderr = stderr;
        err.stdout = stdout;
        reject(err);
      }
    });

    child.on("error", (err) => {
      reject(new Error(`Failed to start coordinator: ${err.message}`));
    });
  });
}

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------
function shutdown(signal) {
  console.log(`\n${signal} received — shutting down...`);

  // Kill all running coordinator processes
  for (const job of jobs.values()) {
    if (job.process) {
      job.status = "cancelled";
      job.phase = "cancelled";
      job.process.kill("SIGTERM");
      job.process = null;
    }
  }

  // Close all WebSocket connections
  for (const client of wss.clients) {
    client.close(1001, "Server shutting down");
  }

  // Stop accepting new connections, then exit
  server.close(() => {
    // Remove PID file
    try { fs.unlinkSync(PID_FILE); } catch {}
    console.log("Server stopped.");
    process.exit(0);
  });

  // Force exit after 5 seconds if graceful shutdown stalls
  setTimeout(() => {
    try { fs.unlinkSync(PID_FILE); } catch {}
    console.error("Forced exit after timeout.");
    process.exit(1);
  }, 5000);
}

if (!DESKTOP_MODE) {
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

if (DESKTOP_MODE) {
  const desktopShutdown = () => {
    for (const job of jobs.values()) {
      if (job.process) {
        try { job.process.kill("SIGTERM"); } catch {}
      }
    }
    try { server.close(); } catch {}
    try { wss.close(); } catch {}
    setTimeout(() => process.exit(0), 2000);
  };
  process.on("SIGTERM", desktopShutdown);
  process.on("SIGINT", desktopShutdown);
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------
const listenCallback = () => {
  if (DESKTOP_MODE) {
    const boundPort = server.address().port;
    process.stdout.write(`__DESKTOP_READY__PORT=${boundPort}\n`);
  } else {
    fs.writeFileSync(PID_FILE, String(process.pid));
    console.log(`Parallel Transcoder web server listening on http://localhost:${PORT} (pid ${process.pid})`);
  }
};

if (HOST) {
  server.listen(PORT, HOST, listenCallback);
} else {
  server.listen(PORT, listenCallback);
}
