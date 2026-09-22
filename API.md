# Parallel Video Transcoder — API Reference

## Overview

REST API, WebSocket interface, and cluster endpoints for parallel video transcoding. The server accepts video uploads, runs transcoding jobs using a Rust coordinator with multiple workers (local or distributed across a cluster), and streams real-time progress via WebSocket.

**Base URL:** `http://localhost:3000`

## Authentication

**Required.** The server refuses to start outside desktop mode unless `TRANSCODER_API_KEY` is set.

- **Header only:** `X-API-Key: <key>` on every request. The `?api_key=` query form was removed — keys
  in query strings end up in access logs and `Referer` headers.
- **Admin routes** additionally require `X-Admin-Key: <key>`: `GET /api/cluster/workers`,
  `POST /api/cluster/workers/scale`, `DELETE /api/jobs`. When the server has no admin key configured
  these return `403 {"error":"Admin key not configured"}`; on a mismatch,
  `403 {"error":"Admin privileges required"}`.
- **Unauthenticated:** `GET /api/health` and `GET /api/ready` only, because Kubernetes probes cannot
  send headers.
- **WebSocket:** the first frame must be `{"type":"auth","key":"<key>"}`. The server replies
  `{"type":"auth-ok"}`, or closes with code `4401` — immediately on a bad key, or after
  `TRANSCODER_WS_AUTH_TIMEOUT_MS` (default 5000) of silence. Messages sent before authenticating are
  ignored and close the socket.
- **Downloads** are browser navigations and cannot carry a header, so they use a one-shot ticket:
  `POST /api/download-ticket` → `GET /api/download/:jobId/:filename?ticket=…`. The download route
  also accepts `X-API-Key` directly for scripted clients.

`DESKTOP_MODE=1` (set by the desktop app, which binds loopback) disables all of the above.

---

## Transcoding Endpoints

### Health Check
`GET /api/health`

Liveness. Unauthenticated, and deliberately free of version, platform, uptime and job counts — those
were reconnaissance for an anonymous caller and moved to `/api/capabilities`.

**Response:**
```json
{
  "status": "ok",
  "localMode": false,
  "authRequired": true
}
```

`localMode` is `false` when the image ships no coordinator binary, so local transcoding is
unavailable. `authRequired` is `false` only in desktop mode; the web UI uses it to decide whether to
prompt for a key.

### Readiness
`GET /api/ready`

Unauthenticated. Fails only on conditions that make *this* pod unable to serve.

**Response:** `200 { "ready": true, "objectStore": true }`, or
`503 { "ready": false, "reason": "disk", "objectStore": true }` when free space under the upload
directory is below 1 GiB.

`objectStore` reports bucket reachability but does **not** fail the probe: the deployment runs a
single replica, so a transient bucket error would pull the only backend and take the UI, job list
and downloads down with it — none of which need the object store.

### Capabilities
`GET /api/capabilities`

Authenticated. Host detail the UI needs to pick encoders.

**Response:**
```json
{
  "platform": "darwin",
  "arch": "arm64",
  "localMode": false
}
```

### Upload Video
`POST /api/upload`

Upload a video file (multipart/form-data, field name `video`). Max size: 10 GB.

**Response:**
```json
{
  "uploadId": "my_video_1709654400000.mp4",
  "originalName": "my_video.mp4",
  "size": 104857600
}
```

**Example:**
```bash
curl -X POST http://localhost:3000/api/upload -F "video=@video.mp4"
```

### Import Video from URL
`POST /api/url-import`

Download a video from a remote URL into the server's upload directory. Follows up to 5 redirects. HTTP and HTTPS only.

The target is resolved and checked before connecting, and again after every redirect. Anything
resolving to loopback, RFC1918, link-local (including `169.254.169.254`), carrier-grade NAT, the
unspecified block, or their IPv6 and IPv4-mapped equivalents — and any non-`http(s)` scheme — returns
`400 {"error":"URL not allowed"}`. All rejections share that one message so the endpoint cannot be
used to probe which internal hosts exist. The connection is made to the resolved address with the
original `Host`/SNI, closing the DNS-rebinding window, and the body is capped at the upload limit.

**Request body (JSON):**
```json
{
  "url": "https://example.com/video.mp4"
}
```

**Response:**
```json
{
  "uploadId": "video_1709654400000.mp4",
  "originalName": "video.mp4",
  "size": 104857600,
  "source": "url"
}
```

**Example:**
```bash
curl -X POST http://localhost:3000/api/url-import \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com/video.mp4"}'
```

### Start Transcode Job
`POST /api/transcode`

Returns `501 {"error":"Local transcoding is not available in this deployment"}` where the image
ships no coordinator binary (check `localMode` on `/api/health`).

Every field is validated against a strict allowlist before anything is spawned. The first failure
returns `400 { "error": "...", "field": "<name>" }`.

**Request body (JSON):**

| Field | Type | Default | Accepted values |
|-------|------|---------|-------------|
| `uploadId` | string | *required* | Non-empty ID from upload |
| `format` | string | `"hls"` | `"hls"` or `"mp4"` |
| `mode` | string | `"normal"` | `"normal"`, `"copy"`, `"smart"`, `"smart-auto"` |
| `crf` | number | `23` | Integer 0–63 |
| `preset` | string | `"medium"` | `"ultrafast"`…`"veryslow"`, or `"0"`–`"12"` for SVT-AV1 |
| `encoder` | string | `"libx264"` | One of the 11 encoders below — nothing else reaches argv |
| `workers` | number | `0` | Integer 0 to `min(64, CPU count)`; 0 = auto-detect |
| `smartTolerance` | number | `0.3` | 0.05–1.0 |
| `verbose` | boolean | `false` | Boolean only |

**Supported encoders:**

| Codec | CPU | macOS GPU | Linux NVIDIA | Linux Intel/AMD |
|-------|-----|-----------|-------------|-----------------|
| H.264 | `libx264` | `h264_videotoolbox` | `h264_nvenc` | `h264_vaapi` |
| H.265 | `libx265` | `hevc_videotoolbox` | `hevc_nvenc` | `hevc_vaapi` |
| AV1   | `libsvtav1`, `libaom-av1` | — | `av1_nvenc` | — |

**Response:**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running"
}
```

**Example:**
```bash
curl -X POST http://localhost:3000/api/transcode \
  -H "Content-Type: application/json" \
  -d '{"uploadId": "my_video_1709654400000.mp4", "format": "mp4", "encoder": "libx265", "crf": 28}'
```

### Analyze Video
`POST /api/analyze`

Analyze complexity without encoding. Returns
`501 {"error":"Local transcoding is not available in this deployment"}` where the image ships no
coordinator binary (check `localMode` on `/api/health`).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `uploadId` | string | *required* | Non-empty ID from upload |
| `crf` | number | `23` | Integer 0–63 |
| `encoder` | string | `"libx264"` | One of the 11 encoders listed above |
| `smartTolerance` | number | `0.3` | 0.05–1.0 |

### List Jobs
`GET /api/jobs`

```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "running",
    "phase": "encoding",
    "percent": 65,
    "config": { "format": "mp4", "crf": 28, "preset": "fast", "encoder": "libx265" },
    "createdAt": "2024-03-05T10:30:00.000Z",
    "logCount": 42
  }
]
```

### Get Job Status
`GET /api/jobs/:id`

**Phases:** `starting` → `analyzing` → `splitting` → `encoding` → `finalizing` → `complete`

**Statuses:** `queued`, `running`, `complete`, `error`, `cancelled`

### Get Job Logs
`GET /api/jobs/:id/logs?offset=0&limit=100`

```json
{
  "total": 142,
  "offset": 0,
  "lines": ["2024-03-05T10:30:00 INFO coordinator: Analyzing video..."]
}
```

### List Output Files
`GET /api/jobs/:id/files`

```json
[
  { "name": "output.mp4", "size": 52428800 }
]
```

### Download Ticket
`POST /api/download-ticket`

A browser download is a plain navigation and cannot carry `X-API-Key`, so the key is traded for a
short-lived grant instead of being put back into a query string.

**Request body:** `{ "jobId": "JOB_ID", "filename": "output.mp4" }`

**Response:** `{ "ticket": "<64 hex chars>" }` — valid 60 seconds, single use, bound to that one
`jobId` + `filename`. A request for a different file does not consume it. `404` when the job has no
output directory yet.

### Download Output File
`GET /api/download/:jobId/:filename?ticket=<ticket>`

Accepts either a `?ticket=` from the endpoint above or an `X-API-Key` header, so scripted clients do
not need a ticket. `404` when the job exists but has produced no output yet — cluster jobs have no
output directory until assembly finishes.

### Cancel Job
`DELETE /api/jobs/:id`

```json
{ "deleted": true }
```

---

## Cluster Endpoints

These endpoints interact with a running cluster of `transcoder-node` daemons.

All cluster endpoints accept an optional `master` parameter (query string for `GET`, JSON body for `POST`) so the UI can talk to any reachable master without restarting the server. When omitted, the server falls back to the `CLUSTER_MASTER` environment variable (default `localhost:9900`).

```
GET  /api/cluster/status?master=host:port
GET  /api/cluster/nodes?master=host:port
POST /api/cluster/transcode     { "master": "host:port", ... }
```

### Cluster Status
`GET /api/cluster/status`

Returns the current cluster state from the master node.

**Response:**
```json
{
  "master": "node-1",
  "nodes": 3,
  "activeJobs": 1,
  "totalCapacity": { "cores": 32, "memoryGb": 96 }
}
```

### List Cluster Nodes
`GET /api/cluster/nodes`

Returns information about all nodes in the cluster.

**Response:**
```json
[
  {
    "id": "550e8400-...",
    "name": "node-1",
    "address": "192.168.1.10:9000",
    "role": "master",
    "capabilities": {
      "cpuCores": 8,
      "memoryMb": 32768,
      "gpus": ["videotoolbox"]
    },
    "load": { "activeTasks": 2, "cpuPercent": 45.2 },
    "status": "alive"
  }
]
```

### Submit Cluster Job
`POST /api/cluster/transcode`

Submit a transcoding job to the cluster. The master node distributes segments across all available nodes.

The master's assembly path always produces MP4, so `format` must be `"mp4"`. Anything else returns
`400 {"error":"Cluster mode produces MP4 only","field":"format"}` rather than silently handing back a
different container.

**Request body (JSON):**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `uploadId` | string | *required* | ID from upload |
| `format` | string | `"mp4"` | Must be `"mp4"` |
| `encoder` | string | `"libx264"` | Video encoder |
| `crf` | number | `23` | Quality level, integer 0–63 |
| `preset` | string | `"medium"` | Speed preset |

**Response:**
```json
{
  "jobId": "cluster-job-550e8400",
  "status": "submitted",
  "assignedNodes": ["node-1", "node-2", "node-3"]
}
```

---

## WebSocket API

Connect to `ws://localhost:3000/ws` for real-time updates.

### Authenticate
The **first** frame must be an auth frame. Any other first message, a wrong key, or
`TRANSCODER_WS_AUTH_TIMEOUT_MS` of silence closes the socket with code `4401`.
```json
{ "type": "auth", "key": "YOUR_API_KEY" }
```
The server acknowledges with `{ "type": "auth-ok" }`. In desktop mode no auth frame is needed.

### Subscribe
Only accepted after `auth-ok`.
```json
{ "type": "subscribe", "jobId": "JOB_ID" }
```

### Server Events

**progress:**
```json
{ "type": "progress", "jobId": "...", "phase": "encoding", "percent": 65, "message": "Encoding segment 6/10" }
```

**log:**
```json
{ "type": "log", "jobId": "...", "line": "INFO worker: Segment 6 complete" }
```

**complete:**
```json
{ "type": "complete", "jobId": "...", "outputFiles": [{ "name": "output.mp4", "size": 52428800 }] }
```

**error:**
```json
{ "type": "error", "jobId": "...", "message": "Coordinator exited with code 1" }
```

### JavaScript Example
```javascript
const ws = new WebSocket("ws://localhost:3000/ws");
ws.onopen = () => {
  // Must be the first frame. Anything else closes the socket with code 4401.
  ws.send(JSON.stringify({ type: "auth", key: API_KEY }));
};
ws.onmessage = (e) => {
  const msg = JSON.parse(e.data);
  if (msg.type === "auth-ok") ws.send(JSON.stringify({ type: "subscribe", jobId: "JOB_ID" }));
  if (msg.type === "progress") console.log(`${msg.phase}: ${msg.percent}%`);
  if (msg.type === "complete") console.log("Done!", msg.outputFiles);
  if (msg.type === "error") console.error(msg.message);
};
ws.onclose = (e) => {
  if (e.code === 4401) console.error("API key rejected");
};
```

### Python Example
```python
import asyncio, json, websockets

async def monitor(job_id, api_key):
    async with websockets.connect("ws://localhost:3000/ws") as ws:
        # Authenticate first, then wait for the ack before subscribing.
        await ws.send(json.dumps({"type": "auth", "key": api_key}))
        assert json.loads(await ws.recv())["type"] == "auth-ok"
        await ws.send(json.dumps({"type": "subscribe", "jobId": job_id}))
        async for message in ws:
            msg = json.loads(message)
            if msg["type"] == "complete":
                break
            print(f"{msg.get('phase', '')}: {msg.get('percent', '')}%")

asyncio.run(monitor("JOB_ID", "YOUR_API_KEY"))
```

---

## Cluster OpCode Protocol

The cluster control plane uses an OBS-websocket-inspired binary protocol over WebSocket. Each message has the shape `{ "op": <number>, "d": <payload> }`.

| OpCode | Name | Direction | Description |
|--------|------|-----------|-------------|
| 0 | Hello | Server→Client | Initial handshake with node capabilities |
| 1 | Identify | Client→Server | Node identification and auth |
| 2 | Identified | Server→Client | Successful identification |
| 10 | ElectionStart | Any→All | Bully election initiation |
| 11 | ElectionAlive | Any→Candidate | "I'm alive" response to election |
| 12 | ElectionVictory | Leader→All | New leader announcement |
| 20 | Heartbeat | Any→Peer | Periodic heartbeat |
| 21 | HeartbeatAck | Peer→Any | Heartbeat acknowledgment |
| 30 | JobSubmit | Client→Master | Submit new transcoding job |
| 31 | JobAccepted | Master→Client | Job accepted confirmation |
| 32 | JobProgress | Worker→Master | Segment progress update |
| 33 | JobComplete | Master→Client | Job finished successfully |
| 34 | JobFailed | Master→Client | Job failed |
| 35 | JobCancel | Client→Master | Cancel a running job |
| 40 | SegmentAssign | Master→Worker | Assign segment to worker |
| 41 | SegmentAssignAck | Worker→Master | Segment assignment acknowledged |
| 42 | SegmentComplete | Worker→Master | Segment encoding finished |
| 43 | SegmentFailed | Worker→Master | Segment encoding failed |
| 50 | StatusRequest | Any→Master | Request cluster status |
| 51 | StatusResponse | Master→Any | Cluster status response |
| 60 | Event | Master→Subscribers | Event broadcast |
| 70 | NodeLeave | Any→All | Graceful node departure |
| 255 | Error | Any→Any | Error notification |

---

## Error Responses

All errors return JSON. Messages are deliberately generic — upstream detail (hostnames, paths,
cluster topology) is logged server-side, not returned:
```json
{ "error": "Description of what went wrong" }
```

Parameter rejections additionally name the offending field: `{ "error": "...", "field": "encoder" }`.

| Code | Meaning |
|------|---------|
| 400 | Bad request (missing/invalid parameters, or `URL not allowed`) |
| 401 | Unauthorized (API key missing or wrong; WebSocket close code `4401`) |
| 403 | Forbidden (admin key missing or wrong) |
| 404 | Not found (job, file, or output not yet available) |
| 500 | Internal server error |
| 501 | Not available in this deployment (local transcoding without a coordinator) |
| 503 | Dependency unavailable (cluster unreachable, Kubernetes API absent, not ready) |

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PORT` | `3000` | Server listen port (`0` picks an ephemeral port and logs it) |
| `TRANSCODER_API_KEY` | **required** outside desktop mode | Shared key for every API and WebSocket call. The server exits 1 without it. |
| `TRANSCODER_ADMIN_KEY` | *(none — admin routes return 403 until set)* | Second key for worker scaling and bulk job deletion |
| `CLUSTER_MASTER` | `localhost:9900` | Cluster master `host:port`. Outside desktop mode, per-request overrides are ignored. |
| `TRANSCODER_STATE_DIR` | `web/` | Root for `uploads/`, `outputs/` and the pid file |
| `CORS_ORIGINS` | *(none — no CORS headers at all)* | Comma-separated exact origins. Credentials are never allowed. |
| `TRANSCODER_WS_AUTH_TIMEOUT_MS` | `5000` | How long an unauthenticated WebSocket may live before close code 4401 |
| `DESKTOP_MODE` | *(unset)* | Set to `1` by the desktop app: binds loopback, requires no keys |
