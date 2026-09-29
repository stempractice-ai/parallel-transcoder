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
import { S3Client, PutObjectCommand, GetObjectCommand, HeadBucketCommand } from "@aws-sdk/client-s3";
import * as ociCommon from "oci-common";
import { ContainerEngineClient } from "oci-containerengine";
import { timingSafeMatch } from "./lib/auth.js";
import { validateTranscodeParams } from "./lib/validate.js";
import { assertPublicUrl } from "./lib/ssrf.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RESOURCES_DIR = process.env.TRANSCODER_RESOURCES_DIR || path.join(__dirname, "..");
const COORDINATOR_BIN = path.join(RESOURCES_DIR, "bin", "transcoder-coordinator");
const LIB_DIR = path.join(RESOURCES_DIR, "lib") + path.sep;
// All mutable server state (uploads, outputs, pid file) lives under one root
// so it can be relocated onto a writable volume when the container filesystem
// is read-only, and onto a temp dir in tests.
const STATE_DIR = process.env.TRANSCODER_STATE_DIR || __dirname;
const UPLOAD_DIR = path.join(STATE_DIR, "uploads");
const OUTPUT_DIR = path.join(STATE_DIR, "outputs");

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
      // Default checksum behaviour makes the SDK wrap streamed bodies in
      // aws-chunked transfer encoding, which OCI's S3-compatible endpoint
      // rejects. Disabling it is what allows a stream body at all — without
      // this the only workaround is buffering the whole object in memory.
      requestChecksumCalculation: "WHEN_REQUIRED",
      responseChecksumValidation: "WHEN_REQUIRED",
    })
  : null;

// In-cluster Kubernetes API access (worker pod scaling). Only usable when
// this server itself runs as a pod with a mounted ServiceAccount token —
// i.e. real k8s deployments (OCI, kind), never local/desktop mode. Talks to
// the target object's /scale subresource directly over HTTPS so the image
// doesn't need a bundled kubectl binary.
//
// The worker topology differs by overlay: the kind dev overlay bakes master
// (ordinal 0) + workers into one StatefulSet, so scaling down must stop at
// 1. The OCI overlay runs workers as their own Deployment
// (transcoder-worker-cpu) separate from the master StatefulSet, so it can
// scale all the way to 0. K8S_WORKER_KIND/NAME point at whichever object
// this deployment actually uses; K8S_WORKER_MIN_REPLICAS defaults per-kind
// but can be overridden explicitly.
const K8S_SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount";
const K8S_TOKEN_PATH = path.join(K8S_SA_DIR, "token");
const K8S_CA_PATH = path.join(K8S_SA_DIR, "ca.crt");
const K8S_NAMESPACE_PATH = path.join(K8S_SA_DIR, "namespace");
const K8S_WORKER_KIND = (process.env.K8S_WORKER_KIND || "statefulsets").toLowerCase();
const K8S_WORKER_NAME = process.env.K8S_WORKER_NAME || "transcoder-node";
const K8S_WORKER_MIN_REPLICAS = process.env.K8S_WORKER_MIN_REPLICAS != null
  ? Number(process.env.K8S_WORKER_MIN_REPLICAS)
  : (K8S_WORKER_KIND === "statefulsets" ? 1 : 0);
const K8S_AVAILABLE = !!process.env.KUBERNETES_SERVICE_HOST
  && fs.existsSync(K8S_TOKEN_PATH) && fs.existsSync(K8S_CA_PATH);
const K8S_NAMESPACE = K8S_AVAILABLE && fs.existsSync(K8S_NAMESPACE_PATH)
  ? fs.readFileSync(K8S_NAMESPACE_PATH, "utf8").trim()
  : "default";

// OCI virtual-node-pool resizing (OCI overlay only). Scaling the
// transcoder-worker-cpu Deployment alone doesn't shrink/grow the
// underlying OKE Virtual Node Pool — OCI bills for every provisioned
// virtual node regardless of whether a pod is scheduled on it, so a
// deployment scaled to 0 workers still leaves idle (billed) nodes behind
// unless the pool itself is resized too. OCI_WORKER_POOL_BASELINE is the
// pool headroom that must always exist for the always-on master + web
// pods (never touched by worker scaling); every /api/cluster/workers/scale
// call resizes the pool to baseline + the exact replica count the Web UI
// just requested.
const OCI_SCALER_DIR = "/var/run/secrets/oci-scaler";
const OCI_TENANCY_ID = process.env.OCI_TENANCY_ID || null;
const OCI_USER_ID = process.env.OCI_USER_ID || null;
const OCI_FINGERPRINT = process.env.OCI_FINGERPRINT || null;
const OCI_REGION = process.env.OCI_REGION || null;
const OCI_VIRTUAL_NODE_POOL_ID = process.env.OCI_VIRTUAL_NODE_POOL_ID || null;
const OCI_PRIVATE_KEY_PATH = path.join(OCI_SCALER_DIR, "private-key");
const OCI_WORKER_POOL_BASELINE = process.env.OCI_WORKER_POOL_BASELINE != null
  ? Number(process.env.OCI_WORKER_POOL_BASELINE)
  : 2; // transcoder-node-0 (master) + transcoder-web, always kept warm
const OCI_POOL_RESIZE_AVAILABLE = !!OCI_TENANCY_ID && !!OCI_USER_ID
  && !!OCI_FINGERPRINT && !!OCI_REGION && !!OCI_VIRTUAL_NODE_POOL_ID
  && fs.existsSync(OCI_PRIVATE_KEY_PATH);

let containerEngineClient = null;
function getContainerEngineClient() {
  if (containerEngineClient) return containerEngineClient;
  const privateKey = fs.readFileSync(OCI_PRIVATE_KEY_PATH, "utf8");
  const provider = new ociCommon.SimpleAuthenticationDetailsProvider(
    OCI_TENANCY_ID,
    OCI_USER_ID,
    OCI_FINGERPRINT,
    privateKey,
    null,
    ociCommon.Region.fromRegionId(OCI_REGION),
  );
  containerEngineClient = new ContainerEngineClient({ authenticationDetailsProvider: provider });
  return containerEngineClient;
}

// A pool resize has been observed taking 5-10+ minutes end to end in this
// cluster, so a short retry budget just times out mid-operation instead of
// ever recovering — 30 attempts at 20s covers ~10 minutes.
const POOL_RESIZE_RETRY_ATTEMPTS = 30;
const POOL_RESIZE_RETRY_DELAY_MS = 20 * 1000;

/**
 * Resize the OKE virtual-node-pool to exactly `size` nodes. Fire-and-forget
 * from the caller's perspective — OCI's update call only *submits* the
 * resize (it returns before the pool finishes scaling, same as the rest of
 * this file's async k8s calls), and OCI itself handles graceful pod
 * draining on scale-down, so there's no need to wait for it here.
 *
 * A pool resize takes OCI several minutes to complete, and it rejects any
 * second update submitted while one is still in flight ("... is currently
 * being modified"). That's a real scenario, not just a testing artifact —
 * a user clicking Stop shortly after Start hits it directly. Retry on that
 * specific conflict so the *last* requested size eventually wins instead of
 * silently stranding the pool at whatever size the in-flight update was
 * targeting.
 */
async function resizeVirtualNodePool(size) {
  if (!OCI_POOL_RESIZE_AVAILABLE) return;
  const client = getContainerEngineClient();
  for (let attempt = 1; attempt <= POOL_RESIZE_RETRY_ATTEMPTS; attempt++) {
    try {
      await client.updateVirtualNodePool({
        virtualNodePoolId: OCI_VIRTUAL_NODE_POOL_ID,
        updateVirtualNodePoolDetails: { size },
      });
      return;
    } catch (err) {
      const conflict = /currently being modified/i.test(err.message || "");
      if (!conflict || attempt === POOL_RESIZE_RETRY_ATTEMPTS) throw err;
      await new Promise((resolve) => setTimeout(resolve, POOL_RESIZE_RETRY_DELAY_MS));
    }
  }
}

/** Issue an authenticated request against the in-cluster Kubernetes API server. */
function k8sRequest(method, apiPath, body) {
  return new Promise((resolve, reject) => {
    if (!K8S_AVAILABLE) {
      return reject(new Error("Kubernetes API not available (not running in-cluster)"));
    }
    let token, ca;
    try {
      token = fs.readFileSync(K8S_TOKEN_PATH, "utf8").trim();
      ca = fs.readFileSync(K8S_CA_PATH);
    } catch (err) {
      return reject(err);
    }
    const payload = body ? Buffer.from(JSON.stringify(body)) : null;
    const req = https.request({
      host: process.env.KUBERNETES_SERVICE_HOST,
      port: process.env.KUBERNETES_SERVICE_PORT || "443",
      path: apiPath,
      method,
      ca,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
        ...(payload ? {
          "Content-Type": "application/merge-patch+json",
          "Content-Length": payload.length,
        } : {}),
      },
    }, (res) => {
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        let parsed = null;
        try { parsed = data ? JSON.parse(data) : null; } catch { /* non-JSON error body */ }
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(parsed);
        } else {
          reject(new Error((parsed && parsed.message) || `Kubernetes API returned ${res.statusCode}`));
        }
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function validateWorkerReplicas(replicas) {
  const n = Number(replicas);
  if (!Number.isFinite(n) || n < K8S_WORKER_MIN_REPLICAS || n > 50 || Math.floor(n) !== n) {
    throw new Error(`replicas must be an integer between ${K8S_WORKER_MIN_REPLICAS} and 50`);
  }
  return n;
}

function workerScalePath() {
  return `/apis/apps/v1/namespaces/${K8S_NAMESPACE}/${K8S_WORKER_KIND}/${K8S_WORKER_NAME}/scale`;
}

/**
 * Current desired/current replica counts for the worker StatefulSet/Deployment.
 * The scale subresource's status.replicas is the *total* pod count the
 * controller currently reports (may still be starting up) — not a readiness
 * count, which isn't exposed on this subresource.
 */
async function getWorkerReplicas() {
  const scale = await k8sRequest("GET", workerScalePath());
  // Kubernetes' Go JSON encoding elides int fields at their zero value
  // (`omitempty`), so spec.replicas/status.replicas are simply absent from
  // the response when scaled to 0 — not present-and-0. Default explicitly.
  return {
    replicas: scale.spec.replicas ?? 0,
    currentReplicas: scale.status.replicas ?? 0,
  };
}

/**
 * Scale worker pods to exactly `replicas` (never below K8S_WORKER_MIN_REPLICAS).
 * Also resizes the OCI virtual-node-pool to baseline + replicas, using the
 * exact same replica count the caller (the Web UI's Start/Stop Workers
 * buttons) requested — so the pool always matches actual desired worker
 * capacity instead of drifting from whatever it happened to be created at.
 */
// A resize takes minutes. Concurrent scale requests used to start one
// long-running resize chain each, racing to set different pool sizes; now the
// latest requested size simply supersedes the pending one and exactly one
// chain is ever in flight.
let resizeDesiredSize = null;
let resizeRunning = false;

async function runPoolResizes() {
  if (resizeRunning) return;
  resizeRunning = true;
  try {
    while (resizeDesiredSize !== null) {
      const target = resizeDesiredSize;
      try {
        await resizeVirtualNodePool(target);
        console.log(`[cluster] virtual-node-pool resize to ${target} submitted`);
      } catch (err) {
        console.error(`[cluster] virtual-node-pool resize to ${target} failed:`, err.message);
      }
      // Another request may have landed while this one ran; only stop once the
      // target has stopped moving.
      if (resizeDesiredSize === target) resizeDesiredSize = null;
    }
  } finally {
    resizeRunning = false;
  }
}

async function scaleWorkers(replicas) {
  const n = validateWorkerReplicas(replicas);
  await k8sRequest("PATCH", workerScalePath(), { spec: { replicas: n } });
  if (OCI_POOL_RESIZE_AVAILABLE) {
    resizeDesiredSize = OCI_WORKER_POOL_BASELINE + n;
    runPoolResizes();
  }
  return n;
}

/** Upload the local source file to the object store; returns an s3:// URI. */
async function uploadSourceToObjectStore(jobId, inputPath) {
  const key = `jobs/${jobId}/source/${path.basename(inputPath)}`;
  // Streamed with an explicit ContentLength: buffering the source put the
  // whole file in RSS, and anything past ~150 MB OOM-killed the 256Mi pod,
  // taking every in-memory job record with it.
  const { size } = await fsp.stat(inputPath);
  await s3Client.send(new PutObjectCommand({
    Bucket: OBJECT_STORE_BUCKET,
    Key: key,
    Body: fs.createReadStream(inputPath),
    ContentLength: size,
  }));
  return `s3://${OBJECT_STORE_BUCKET}/${key}`;
}

/** Download an s3://bucket/key object to a local path. */
async function downloadFromObjectStore(uri, destPath) {
  const match = uri.match(/^s3:\/\/([^/]+)\/(.+)$/);
  if (!match) throw new Error(`Not an s3:// URI: ${uri}`);
  const [, bucket, key] = match;
  const resp = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const bytes = await resp.Body.transformToByteArray();
  await fsp.writeFile(destPath, bytes);
}

/**
 * Fetch a completed cluster job's assembled output (if the master produced
 * one) into the job's local output dir so it's downloadable through the
 * same /api/download path local jobs already use.
 */
async function finalizeClusterJob(jobId, job, outputUri) {
  if (!outputUri || !s3Client) {
    broadcast(jobId, { type: "complete", jobId, outputFiles: [] });
    return;
  }
  try {
    const jobOutputDir = path.join(OUTPUT_DIR, jobId);
    await fsp.mkdir(jobOutputDir, { recursive: true });
    const destPath = path.join(jobOutputDir, "output.mp4");
    await downloadFromObjectStore(outputUri, destPath);
    job.outputDir = jobOutputDir;
    const stat = await fsp.stat(destPath);
    broadcast(jobId, { type: "complete", jobId, outputFiles: [{ name: "output.mp4", size: stat.size }] });
  } catch (err) {
    console.error(`Failed to fetch assembled output for job ${jobId}:`, err.message);
    broadcast(jobId, { type: "complete", jobId, outputFiles: [] });
  }
}

/**
 * Resolve the cluster master for a request.
 *
 * Caller-supplied overrides are honoured only in desktop mode, where the user
 * owns the machine. On a server they would let anyone aim the outbound
 * WebSocket at any internal host, and the distinct failure strings that
 * produced made a reliable port scanner.
 */
function resolveMaster(req) {
  if (!DESKTOP_MODE) return DEFAULT_CLUSTER_MASTER;
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
const PID_FILE = path.join(STATE_DIR, ".web.pid");

// The production web image deliberately ships no coordinator binary, so the
// local (non-cluster) transcode paths cannot work there. Detect once rather
// than letting every request fail with a bare spawn ENOENT.
const LOCAL_MODE_AVAILABLE = fs.existsSync(COORDINATOR_BIN);

// Browsers cannot set headers on a WebSocket handshake, so the first frame
// must carry the key. This bounds how long an unauthenticated socket may live.
const WS_AUTH_TIMEOUT_MS = Number(process.env.TRANSCODER_WS_AUTH_TIMEOUT_MS || 5000);

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

// Security response headers. The SPA is a single file with one inline
// <script>, one inline <style> and inline style attributes, so 'unsafe-inline'
// is required until that markup is split out; the font hosts are the only
// third-party origins it references.
const CONTENT_SECURITY_POLICY = [
  "default-src 'self'",
  "script-src 'self' 'unsafe-inline'",
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
  "font-src https://fonts.gstatic.com",
  "img-src 'self' data:",
  "connect-src 'self'",
  "frame-ancestors 'none'",
].join("; ");

app.disable("x-powered-by");
app.use((_req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  res.setHeader("Content-Security-Policy", CONTENT_SECURITY_POLICY);
  next();
});

// CORS is opt-in and never reflective. The shipped SPA is same-origin, so by
// default no CORS headers are emitted at all; CORS_ORIGINS holds an exact
// allowlist for any out-of-band client. Credentials are never allowed.
const CORS_ORIGINS = new Set(
  (process.env.CORS_ORIGINS || "").split(",").map((o) => o.trim()).filter(Boolean),
);
if (!DESKTOP_MODE && CORS_ORIGINS.size > 0) {
  app.use((req, res, next) => {
    res.header("Vary", "Origin");
    if (req.headers.origin && CORS_ORIGINS.has(req.headers.origin)) {
      res.header("Access-Control-Allow-Origin", req.headers.origin);
      res.header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
      res.header("Access-Control-Allow-Headers", "Content-Type, X-API-Key, X-Admin-Key");
    }
    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
  });
}

// API key authentication. Fails closed: outside desktop mode the server
// refuses to start without a key rather than serving the API anonymously.
const API_KEY = process.env.TRANSCODER_API_KEY || null;
const ADMIN_KEY = process.env.TRANSCODER_ADMIN_KEY || null;
if (!DESKTOP_MODE && !API_KEY) {
  console.error("TRANSCODER_API_KEY is required outside desktop mode");
  process.exit(1);
}

app.use("/api", (req, res, next) => {
  if (DESKTOP_MODE) return next();
  // Kubernetes probes cannot send headers, so the two probe paths stay open.
  if (req.path === "/health" || req.path === "/ready") return next();
  // Downloads are <a href> navigations and cannot carry a header; that route
  // authenticates itself, accepting either the key or a one-shot ticket.
  if (req.path.startsWith("/download/")) return next();
  if (timingSafeMatch(req.headers["x-api-key"], API_KEY)) return next();
  res.status(401).json({ error: "Invalid or missing API key" });
});

/**
 * Second factor for destructive and cost-bearing routes (worker scaling, bulk
 * job deletion). Fails closed when no admin key is configured.
 */
function requireAdmin(req, res, next) {
  if (DESKTOP_MODE) return next();
  if (!ADMIN_KEY) return res.status(403).json({ error: "Admin key not configured" });
  if (timingSafeMatch(req.headers["x-admin-key"], ADMIN_KEY)) return next();
  res.status(403).json({ error: "Admin privileges required" });
}

// Short-lived, single-use download grants. A browser download is a plain
// navigation, so the alternative would be putting the API key back into query
// strings — where it lands in access logs and Referer headers.
const DOWNLOAD_TICKET_TTL_MS = 60_000;
/** @type {Map<string, {jobId: string, filename: string, expires: number}>} */
const downloadTickets = new Map();

function issueDownloadTicket(jobId, filename) {
  const now = Date.now();
  for (const [id, t] of downloadTickets) {
    if (t.expires <= now) downloadTickets.delete(id);
  }
  const ticket = crypto.randomBytes(32).toString("hex");
  downloadTickets.set(ticket, { jobId, filename, expires: now + DOWNLOAD_TICKET_TTL_MS });
  return ticket;
}

/**
 * True when `ticket` grants exactly this file, consuming it.
 *
 * A mismatched request must not spend the grant: otherwise any guessed URL
 * burns a legitimate user's ticket before they can use it.
 */
function redeemDownloadTicket(ticket, jobId, filename) {
  if (typeof ticket !== "string") return false;
  const entry = downloadTickets.get(ticket);
  if (!entry) return false;
  if (entry.expires <= Date.now()) {
    downloadTickets.delete(ticket);
    return false;
  }
  if (entry.jobId !== jobId || entry.filename !== filename) return false;
  downloadTickets.delete(ticket);
  return true;
}

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

// Liveness. Unauthenticated because kubelet cannot present a key; the body is
// deliberately free of version, platform and job counts, which are
// reconnaissance for an unauthenticated caller.
app.get("/api/health", (_req, res) => {
  // authRequired lets the shipped SPA — which is the same file in the desktop
  // build, where no key exists — decide whether to prompt and whether a
  // WebSocket may be opened at all.
  res.json({ status: "ok", localMode: LOCAL_MODE_AVAILABLE, authRequired: !DESKTOP_MODE });
});

// Readiness. Only conditions that make *this* pod unable to serve flip it
// unready.
//
// Object-store reachability is reported but deliberately does NOT fail the
// probe: the deployment runs a single replica behind the load balancer
// (web-frontend.yaml `replicas: 1`), so a transient bucket error would pull
// the only backend and turn a partial degradation — cluster submission is the
// one feature that needs S3 — into a total outage of the UI, job list and
// downloads. Revisit once there is more than one replica and a PDB.
const READY_CACHE_MS = 10_000;
const MIN_FREE_BYTES = 1024 * 1024 * 1024; // 1 GiB
let bucketProbe = { at: 0, ok: true };

async function objectStoreReachable() {
  if (!s3Client) return true;
  const now = Date.now();
  // The readiness probe runs every few seconds; one HeadBucket per probe would
  // be pure overhead against the object store.
  if (now - bucketProbe.at < READY_CACHE_MS) return bucketProbe.ok;
  let ok = false;
  try {
    await s3Client.send(new HeadBucketCommand({ Bucket: OBJECT_STORE_BUCKET }));
    ok = true;
  } catch (err) {
    console.error("[ready] object store unreachable:", err.message);
  }
  bucketProbe = { at: now, ok };
  return ok;
}

app.get("/api/ready", async (_req, res) => {
  const objectStore = await objectStoreReachable();
  try {
    const fsStat = await fsp.statfs(UPLOAD_DIR);
    // Out of disk is local and fatal: uploads and outputs both fail.
    if (fsStat.bavail * fsStat.bsize < MIN_FREE_BYTES) {
      return res.status(503).json({ ready: false, reason: "disk", objectStore });
    }
  } catch (err) {
    console.error("[ready] statfs failed:", err.message);
    return res.status(503).json({ ready: false, reason: "disk", objectStore });
  }
  res.json({ ready: true, objectStore });
});

// Host detail the UI needs to pick encoders. Authenticated — this is the
// information /api/health used to hand out anonymously.
app.get("/api/capabilities", (_req, res) => {
  res.json({
    platform: process.platform,
    arch: process.arch,
    localMode: LOCAL_MODE_AVAILABLE,
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

  // Scheme check plus DNS resolution against the blocked-range list. Rejections
  // are deliberately indistinguishable from one another so the endpoint cannot
  // be used to probe which internal hosts exist.
  let resolved;
  try {
    resolved = await assertPublicUrl(url);
  } catch {
    return res.status(400).json({ error: "URL not allowed" });
  }

  // Derive filename from URL path or use a generic name
  const urlPath = new URL(url).pathname.split("/").pop() || "video";
  const safeName = urlPath.replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 200);
  const ext = path.extname(safeName) || ".mp4";
  const base = path.basename(safeName, ext);
  const filename = `${base}_${Date.now()}${ext}`;
  const destPath = path.join(UPLOAD_DIR, filename);

  try {
    await downloadFile(url, destPath, 5, resolved);
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
    if (err && err.blocked) {
      return res.status(400).json({ error: "URL not allowed" });
    }
    console.error(`[url-import] download failed for ${filename}:`, err.message);
    res.status(500).json({ error: "Download failed" });
  }
});

// Start transcode job
app.post("/api/transcode", async (req, res) => {
  // Short-circuit before any mkdir: the production web image ships no
  // coordinator, and the old path left orphaned job directories behind.
  if (!LOCAL_MODE_AVAILABLE) {
    return res.status(501).json({ error: "Local transcoding is not available in this deployment" });
  }

  const invalid = validateTranscodeParams(req.body);
  if (invalid) return res.status(400).json(invalid);

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
  if (!LOCAL_MODE_AVAILABLE) {
    return res.status(501).json({ error: "Local transcoding is not available in this deployment" });
  }

  const invalid = validateTranscodeParams(req.body);
  if (invalid) return res.status(400).json(invalid);

  const {
    uploadId,
    crf = 23,
    encoder = "libx264",
    smartTolerance = 0.3,
  } = req.body;

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
    console.error("[analyze] failed:", err.message);
    res.status(500).json({ error: "Analyze failed" });
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

// Delete all jobs and their stored files (uploaded source + output dir)
app.delete("/api/jobs", requireAdmin, async (_req, res) => {
  let count = 0;
  for (const job of jobs.values()) {
    if (job.process) {
      job.process.kill("SIGTERM");
      job.process = null;
    }
    if (job.outputDir) {
      fsp.rm(job.outputDir, { recursive: true, force: true }).catch(() => {});
    }
    if (job.config && job.config.uploadId) {
      const srcPath = path.join(UPLOAD_DIR, path.basename(job.config.uploadId));
      fsp.rm(srcPath, { force: true }).catch(() => {});
    }
    subscribers.delete(job.id);
    count++;
  }
  jobs.clear();
  res.json({ deleted: count });
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
  if (!job.outputDir) return res.status(404).json({ error: "Job output not available" });
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

// Trade an API key for a one-shot grant on a single output file, so the UI can
// hand the browser an ordinary download link.
app.post("/api/download-ticket", (req, res) => {
  const { jobId, filename } = req.body || {};
  if (typeof jobId !== "string" || typeof filename !== "string") {
    return res.status(400).json({ error: "jobId and filename are required" });
  }
  const job = jobs.get(jobId);
  if (!job || !job.outputDir) return res.status(404).json({ error: "Job output not available" });
  res.json({ ticket: issueDownloadTicket(jobId, path.basename(filename)) });
});

// Download output file. Authenticates itself because it is exempt from the
// header middleware: either a valid key header or a matching one-shot ticket.
app.get("/api/download/:jobId/:filename", (req, res) => {
  const filename = path.basename(req.params.filename);
  const authorized = DESKTOP_MODE
    || timingSafeMatch(req.headers["x-api-key"], API_KEY)
    || redeemDownloadTicket(req.query.ticket, req.params.jobId, filename);
  if (!authorized) return res.status(401).json({ error: "Invalid or missing API key" });

  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: "Job not found" });
  // Cluster jobs have no outputDir until finalizeClusterJob runs; joining null
  // threw a TypeError and surfaced as a 500.
  if (!job.outputDir) return res.status(404).json({ error: "Job output not available" });

  // Path join safety: ensure filename doesn't escape the output dir
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
    console.error("[cluster] status query failed:", err.message);
    res.status(503).json({ master, error: "Cluster unreachable" });
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
    console.error("[cluster] nodes query failed:", err.message);
    res.status(503).json({ error: "Cluster unreachable" });
  }
});

// Submit transcode job to the cluster
app.post("/api/cluster/transcode", async (req, res) => {
  let master;
  try { master = resolveMaster(req); }
  catch (err) { return res.status(err.statusCode || 400).json({ error: err.message }); }

  const invalid = validateTranscodeParams(req.body, { cluster: true });
  if (invalid) return res.status(400).json(invalid);

  const {
    uploadId,
    // The master's assembly path always emits MP4; anything else is rejected
    // above rather than silently returning a different container.
    format = "mp4",
    crf = 23,
    preset = "medium",
    encoder = "libx264",
  } = req.body;

  const inputPath = path.join(UPLOAD_DIR, path.basename(uploadId));
  if (!fs.existsSync(inputPath)) {
    return res.status(404).json({ error: "Uploaded file not found" });
  }

  const jobId = crypto.randomUUID();

  /** @type {JobState} */
  const job = {
    id: jobId,
    status: "running",
    process: null,
    config: { uploadId, format, crf, preset, encoder, cluster: true },
    outputDir: null,
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
    job.status = "error";
    job.phase = "error";
    // jobSummary surfaces errorMessage to clients, so the detail has to be
    // kept out of the stored value too, not just the response body.
    console.error(`[cluster] submission failed for job ${jobId}:`, err.message);
    job.errorMessage = "Cluster submission failed";
    broadcast(jobId, { type: "error", jobId, message: job.errorMessage });
    res.status(500).json({ error: "Cluster submission failed" });
  }
});

// Worker pod scaling status (k8s-only — used to show/hide Start/Stop Workers in the UI)
app.get("/api/cluster/workers", requireAdmin, async (_req, res) => {
  if (!K8S_AVAILABLE) return res.json({ available: false });
  try {
    const { replicas, currentReplicas } = await getWorkerReplicas();
    res.json({
      available: true,
      replicas,
      currentReplicas,
      minReplicas: K8S_WORKER_MIN_REPLICAS,
      kind: K8S_WORKER_KIND,
      name: K8S_WORKER_NAME,
      namespace: K8S_NAMESPACE,
      poolResizeAvailable: OCI_POOL_RESIZE_AVAILABLE,
    });
  } catch (err) {
    console.error("[cluster] worker status failed:", err.message);
    res.status(503).json({ available: false, error: "Worker status unavailable" });
  }
});

// Scale worker pods up/down to preserve cost when idle
app.post("/api/cluster/workers/scale", requireAdmin, async (req, res) => {
  if (!K8S_AVAILABLE) return res.status(503).json({ error: "Kubernetes API not available" });
  // Validate before the try so a bad replica count reports its own static
  // message, while infrastructure failures stay generic.
  let requested;
  try {
    requested = validateWorkerReplicas(req.body && req.body.replicas);
  } catch (err) {
    return res.status(400).json({ error: err.message });
  }
  try {
    const n = await scaleWorkers(requested);
    res.json({ ok: true, replicas: n });
  } catch (err) {
    console.error("[cluster] scale failed:", err.message);
    res.status(503).json({ error: "Scale request failed" });
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
  // The handshake cannot carry a header from a browser, so the socket is
  // untrusted until its first frame proves the key — and is dropped if that
  // frame never arrives.
  let authed = DESKTOP_MODE;
  let authTimer = null;
  if (!authed) {
    authTimer = setTimeout(() => ws.close(4401, "unauthorized"), WS_AUTH_TIMEOUT_MS);
  }

  ws.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return; // ignore malformed messages
    }

    if (!authed) {
      // Anything other than a valid auth frame ends the connection; a client
      // that could subscribe first would bypass authentication entirely.
      if (msg.type === "auth" && timingSafeMatch(msg.key, API_KEY)) {
        authed = true;
        clearTimeout(authTimer);
        authTimer = null;
        safeSend(ws, { type: "auth-ok" });
      } else {
        ws.close(4401, "unauthorized");
      }
      return;
    }

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
  });

  ws.on("close", () => {
    clearTimeout(authTimer);
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
        } else if (msg.op === 32 && msg.d.job_id === jobId) {  // JobProgress
          const job = jobs.get(jobId);
          if (!job) return;
          job.segmentsCompleted = msg.d.completed_segments;
          job.segmentsTotal = msg.d.total_segments;
          job.phase = "encoding";
          job.percent = job.segmentsTotal > 0
            ? Math.round((job.segmentsCompleted / job.segmentsTotal) * 100)
            : job.percent;
          broadcast(jobId, {
            type: "progress",
            jobId,
            phase: job.phase,
            percent: job.percent,
            message: `${job.segmentsCompleted}/${job.segmentsTotal} segments complete`,
            startedAt: job.startedAt,
            now: Date.now(),
            segmentsCompleted: job.segmentsCompleted,
            segmentsTotal: job.segmentsTotal,
          });
        } else if (msg.op === 33 && msg.d.job_id === jobId) {  // JobComplete
          const job = jobs.get(jobId);
          if (job) {
            job.status = "complete";
            job.phase = "complete";
            job.percent = 100;
            job.segmentsTotal = msg.d.total_segments || job.segmentsTotal;
            job.segmentsCompleted = job.segmentsTotal;
            finalizeClusterJob(jobId, job, msg.d.output_uri);
          }
          ws.close();
        } else if (msg.op === 34 && msg.d.job_id === jobId) {  // JobFailed
          const job = jobs.get(jobId);
          if (job) {
            job.status = "error";
            job.phase = "error";
            job.errorMessage = msg.d.error || "Cluster job failed";
            broadcast(jobId, { type: "error", jobId, message: job.errorMessage });
          }
          ws.close();
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

/**
 * Download a file from a URL, following redirects (up to 5).
 *
 * Every hop is validated against the blocked-range list and then dialled by
 * resolved IP with the original hostname carried in Host/servername, so a
 * DNS answer cannot change between the check and the connection. The response
 * body is capped at UPLOAD_LIMIT.
 */
function downloadFile(url, destPath, maxRedirects = 5, preResolved = null) {
  const blocked = () => Object.assign(new Error("URL not allowed"), { blocked: true });

  return new Promise((resolve, reject) => {
    const doRequest = async (currentUrl, redirectsLeft, resolved) => {
      let target = resolved;
      if (!target) {
        try {
          target = await assertPublicUrl(currentUrl);
        } catch {
          reject(blocked());
          return;
        }
      }

      const parsed = new URL(currentUrl);
      const isHttps = parsed.protocol === "https:";
      const mod = isHttps ? https : http;
      const pinned = target.addresses[0].address;

      const options = {
        host: pinned,
        port: parsed.port || (isHttps ? 443 : 80),
        path: parsed.pathname + parsed.search,
        headers: { "User-Agent": "ParallelTranscoder/1.0", Host: parsed.host },
        ...(isHttps ? { servername: target.hostname } : {}),
      };

      const req = mod.get(options, (res) => {
        // Follow redirects — re-validated, because the first hop's safety says
        // nothing about where it points.
        if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
          res.resume();
          if (redirectsLeft <= 0) {
            reject(new Error("Too many redirects"));
            return;
          }
          let next;
          try {
            next = new URL(res.headers.location, currentUrl);
          } catch {
            reject(blocked());
            return;
          }
          if (next.protocol !== "http:" && next.protocol !== "https:") {
            reject(blocked());
            return;
          }
          doRequest(next.href, redirectsLeft - 1, null);
          return;
        }

        if (res.statusCode < 200 || res.statusCode >= 300) {
          res.resume();
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }

        const file = fs.createWriteStream(destPath);
        let received = 0;
        res.on("data", (chunk) => {
          received += chunk.length;
          if (received > UPLOAD_LIMIT) {
            res.destroy();
            file.destroy();
            try { fs.unlinkSync(destPath); } catch {}
            reject(new Error("Remote file exceeds the upload limit"));
          }
        });
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
    doRequest(url, maxRedirects, preResolved).catch(reject);
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
    // Containers have no use for a pid file, and the read-only root filesystem
    // the hardened manifests use would make this throw at startup.
    if (!process.env.KUBERNETES_SERVICE_HOST) {
      try { fs.writeFileSync(PID_FILE, String(process.pid)); } catch { /* non-fatal */ }
    }
    // Report the port actually bound, not the configured one — PORT=0 asks the
    // OS for an ephemeral port, and callers need to learn which one it picked.
    console.log(`Parallel Transcoder web server listening on http://localhost:${server.address().port} (pid ${process.pid})`);
  }
};

if (HOST) {
  server.listen(PORT, HOST, listenCallback);
} else {
  server.listen(PORT, listenCallback);
}
