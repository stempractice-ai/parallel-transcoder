# Parallel Video Transcoder

A distributed, multi-node video transcoding engine with intelligent segment allocation, multi-codec support, and hardware-accelerated encoding. Built with Rust for performance and Node.js for the web interface.

## Key Features

- **Distributed Cluster** — Bully-algorithm leader election, WebSocket control plane (OBS-style OpCode protocol), SRT data transport between nodes
- **Multi-Codec** — H.264, H.265/HEVC, and AV1 with CPU and GPU encoders
- **Hardware Acceleration** — VideoToolbox (macOS), NVENC (NVIDIA), VAAPI (Intel/AMD on Linux)
- **Smart Mode** — Complexity-aware encoding that skips segments below a tolerance threshold
- **Parallel Workers** — Keyframe-aligned segmentation with complexity-balanced distribution across cores and machines
- **Web UI** — Dark-themed SPA with drag-and-drop upload, real-time WebSocket progress, and output downloads
- **Desktop App** — Electron wrapper with in-app cluster management: connect to any master, view nodes table, start/stop a local `transcoder-node`, and route transcodes to the cluster with one toggle
- **REST API** — Full programmatic control with optional API key authentication
- **Cross-Platform** — macOS (ARM64/x86_64) and Linux (RHEL, Rocky, Fedora, CentOS)

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Cluster Layer (Rust)                      │
│  transcoder-node daemons on each machine                     │
│  - Bully election → one master, N workers                    │
│  - WebSocket control plane (OBS OpCode protocol)             │
│  - SRT data plane for segment transfer                       │
│  - Complexity-aware scheduler distributes segments           │
└──────────────────────────────────────────────────────────────┘
        ↕ WebSocket (OpCodes)              ↕ SRT (segments)
┌──────────────────────────────────────────────────────────────┐
│                    Coordinator (Rust)                         │
│  - Pre-analyzes video (GOPs, keyframes, scene changes)       │
│  - Splits at keyframe boundaries                             │
│  - Spawns local worker processes or delegates to cluster     │
│  - Reassembles HLS / MP4 output                             │
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│                      Workers (Rust)                           │
│  - Decode assigned segment                                   │
│  - Encode with H.264 / H.265 / AV1 (CPU or GPU)             │
│  - 10-bit pipeline for HEVC and AV1 when source is 10-bit   │
│  - Return encoded segment + metadata                         │
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│                   Web Server (Node.js)                        │
│  - Express REST API + WebSocket live updates                 │
│  - Dark-themed SPA frontend                                  │
│  - Cluster management endpoints                              │
│  - Optional API key authentication                           │
└──────────────────────────────────────────────────────────────┘
```

## Supported Encoders

| Codec | CPU | macOS GPU | Linux GPU (NVIDIA) | Linux GPU (Intel/AMD) |
|-------|-----|-----------|--------------------|-----------------------|
| H.264 | libx264 | h264_videotoolbox | h264_nvenc | h264_vaapi |
| H.265 | libx265 | hevc_videotoolbox | hevc_nvenc | hevc_vaapi |
| AV1   | libsvtav1, libaom-av1 | — | av1_nvenc | — |

## Project Structure

```
parallel-transcoder/
├── cluster/             # Distributed cluster system (Rust)
│   ├── src/
│   │   ├── main.rs      # transcoder-node daemon
│   │   ├── lib.rs       # Library re-exports
│   │   ├── protocol.rs  # OBS-style OpCode message protocol
│   │   ├── transport.rs # WebSocket transport layer
│   │   ├── srt.rs       # SRT data plane (FFmpeg-based)
│   │   ├── election.rs  # Bully algorithm leader election
│   │   ├── node.rs      # Node manager, health monitoring
│   │   └── scheduler.rs # Complexity-aware segment scheduler
│   └── Cargo.toml
├── coordinator/         # Local coordinator binary (Rust)
│   ├── src/
│   │   ├── main.rs      # CLI, orchestration, cluster mode
│   │   ├── analyzer.rs  # Video analysis (keyframes, scenes)
│   │   └── segmenter.rs # Keyframe-aligned segmentation
│   └── Cargo.toml
├── worker/              # Worker binary (Rust)
│   ├── src/
│   │   └── main.rs      # Multi-codec encoding engine
│   └── Cargo.toml
├── web/                 # Web server + UI
│   ├── server.js        # Express + WebSocket + cluster API
│   └── public/
│       └── index.html   # Dark-themed SPA
├── docs/                # Documentation
│   ├── PLAN.md          # Implementation plan
│   └── RESEARCH.md      # Research findings
├── API.md               # REST & WebSocket API reference
├── build.sh             # Cross-platform build script
├── Cargo.toml           # Rust workspace manifest
└── package.json         # Node.js dependencies
```

## Quick Start

### Prerequisites

- **Rust** (latest stable): `rustup update`
- **FFmpeg** development libraries
- **Node.js** v16+

**macOS:**
```bash
brew install ffmpeg
```

**RHEL / Rocky / Fedora:**
```bash
# Enable RPM Fusion for full FFmpeg
sudo dnf install ffmpeg-devel
```

### Build & Run

```bash
# Build all Rust binaries
./build.sh

# Install Node.js dependencies
npm install

# Start the web server
npm run web
# → Open http://localhost:3000
```

### CLI Usage

```bash
# Basic transcode (H.264, 8 workers)
./bin/transcoder-coordinator \
  --input video.mp4 --output out/ \
  --workers 8 --format mp4 --encoder libx264

# H.265 with hardware encoding (macOS)
./bin/transcoder-coordinator \
  --input video.mp4 --output out/ \
  --encoder hevc_videotoolbox --format mp4

# AV1 encoding
./bin/transcoder-coordinator \
  --input video.mp4 --output out/ \
  --encoder libsvtav1 --crf 30

# Smart mode (skip simple segments)
./bin/transcoder-coordinator \
  --input video.mp4 --output out/ \
  --smart --smart-tolerance 0.3

# Analyze without encoding
./bin/transcoder-coordinator \
  --input video.mp4 --output out/ --smart-report
```

### Cluster Mode

```bash
# Start a node (first node becomes master via election)
./bin/transcoder-node --listen 0.0.0.0:9000 --name node-1

# Join existing cluster
./bin/transcoder-node --listen 0.0.0.0:9001 --join 192.168.1.10:9000 --name node-2

# Submit a job through the coordinator in cluster mode
./bin/transcoder-coordinator \
  --input video.mp4 --output out/ \
  --cluster --cluster-master 192.168.1.10:9000
```

## REST API

Full documentation: **[API.md](API.md)**

All `/api/*` routes require `X-API-Key` except `/api/health` and `/api/ready`; the routes marked
**admin** additionally require `X-Admin-Key`.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Liveness — `{ status, localMode, authRequired }`. No key. |
| `/api/ready` | GET | Readiness — 503 when local disk is exhausted. No key. |
| `/api/capabilities` | GET | Server platform and arch, for encoder selection |
| `/api/upload` | POST | Upload video (multipart) |
| `/api/url-import` | POST | Fetch a video from a public URL |
| `/api/transcode` | POST | Start local transcoding job (501 where no coordinator ships) |
| `/api/analyze` | POST | Analyze video complexity (501 where no coordinator ships) |
| `/api/jobs` | GET | List all jobs |
| `/api/jobs` | DELETE | **admin** — delete every job and its files |
| `/api/jobs/:id` | GET | Job status and progress |
| `/api/jobs/:id/logs` | GET | Paginated job logs |
| `/api/jobs/:id/files` | GET | List output files |
| `/api/jobs/:id` | DELETE | Cancel and remove job |
| `/api/download-ticket` | POST | Mint a 60s single-use download grant for one file |
| `/api/download/:jobId/:file` | GET | Download output file (`?ticket=` or `X-API-Key`) |
| `/api/cluster/status` | GET | Cluster status |
| `/api/cluster/nodes` | GET | List cluster nodes |
| `/api/cluster/transcode` | POST | Submit cluster job (MP4 only) |
| `/api/cluster/workers` | GET | **admin** — worker replica status |
| `/api/cluster/workers/scale` | POST | **admin** — scale workers and the node pool |

**WebSocket:** `ws://localhost:3000/ws` — real-time progress, logs, and completion events. The first
frame must be `{"type":"auth","key":"<API key>"}`; the server replies `{"type":"auth-ok"}` or closes
with code `4401`.

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PORT` | `3000` | Server listen port (`0` picks an ephemeral port and logs it) |
| `TRANSCODER_API_KEY` | **required** outside desktop mode | Shared key every API and WebSocket call must present. The server exits 1 without it. |
| `TRANSCODER_ADMIN_KEY` | *(none — admin routes return 403 until set)* | Second key for worker scaling and bulk job deletion |
| `CLUSTER_MASTER` | `localhost:9900` | Cluster master `host:port`. Outside desktop mode this is the only master the server will talk to; per-request overrides are ignored. |
| `TRANSCODER_STATE_DIR` | `web/` | Root for `uploads/`, `outputs/` and the pid file |
| `CORS_ORIGINS` | *(none — no CORS headers at all)* | Comma-separated exact origins allowed to call the API cross-origin. Credentials are never allowed. |
| `TRANSCODER_WS_AUTH_TIMEOUT_MS` | `5000` | How long an unauthenticated WebSocket may live before it is closed with code 4401 |
| `DESKTOP_MODE` | *(unset)* | Set to `1` by the desktop app: binds loopback and requires no keys |

### Authentication keys

Two shared secrets, both generated locally and never committed:

```bash
openssl rand -hex 32   # TRANSCODER_API_KEY   — every client needs this
openssl rand -hex 32   # TRANSCODER_ADMIN_KEY — only whoever may scale workers
```

Run locally:

```bash
TRANSCODER_API_KEY=$(openssl rand -hex 32) \
TRANSCODER_ADMIN_KEY=$(openssl rand -hex 32) \
npm run web
```

Paste the same values into the **Authentication** panel in the web UI. The API key is kept in the
browser's `localStorage`; the admin key only in `sessionStorage`, so it is gone when the tab closes.
The desktop app sets `DESKTOP_MODE=1` and needs neither.

Rotate by changing the environment and restarting the server — every browser then prompts again.
For Kubernetes, see [k8s/README.md](k8s/README.md): the keys come from a Secret that must exist
*before* the pod starts, because the server refuses to boot without one.

### Encoding Parameters

| Parameter | Default | Options |
|-----------|---------|---------|
| `format` | `hls` | `hls`, `mp4` |
| `mode` | `normal` | `normal`, `copy`, `smart`, `smart-auto` |
| `crf` | `23` | 0-51 (H.264/H.265), 0-63 (AV1) |
| `preset` | `medium` | `ultrafast` to `veryslow` |
| `encoder` | `libx264` | See encoder table above |
| `workers` | `0` | 0 = auto-detect CPU cores |

## Benchmark Results

Tested with [Big Buck Bunny](https://peach.blender.org/) (720p H.264, 10 min, 150 MB) on Apple Silicon (M-series, 8 workers, `libx264 -crf 23 -preset fast`).

| Metric | Parallel Transcoder | FFmpeg Baseline |
|--------|-------------------|-----------------|
| **Wall time** | 152.6s | 49.1s* |
| **CPU time** | — | 357s |
| **Output size** | 111 MB | 119 MB |
| **Bitrate** | 1,549 kbps | 1,678 kbps |
| **Output frames** | 14,375 | 14,315 |
| **Throughput** | 94.2 fps | — |

*\*FFmpeg uses internal thread-level parallelism (744% CPU utilization). The parallel transcoder uses process-level segment parallelism — the advantage scales with distributed multi-node clusters where thread-level parallelism cannot reach.*

**Pipeline details:**
- Coordinator analyzed video and detected scene changes across 14,375 frames
- Split into 61 keyframe-aligned segments with complexity balancing
- 8 worker processes encoded segments concurrently
- MP4 mux reassembled all segments — output fully playable, all frames decodable

## Implementation Status

- [x] Rust video analysis, segmentation, and coordinator
- [x] Multi-worker parallel encoding pipeline
- [x] Multi-codec support (H.264, H.265, AV1)
- [x] Hardware acceleration (VideoToolbox, NVENC, VAAPI)
- [x] Distributed cluster with leader election
- [x] OBS-style WebSocket control plane
- [x] SRT data plane for segment transfer
- [x] Complexity-aware cluster scheduler
- [x] Web UI with real-time progress
- [x] REST API with CORS and optional auth
- [x] Cross-platform build system (macOS + Linux RHEL)
- [x] Smart mode and analysis reporting

## License

This project is licensed under the [GNU Lesser General Public License v2.0](https://www.gnu.org/licenses/old-licenses/lgpl-2.0.html) (LGPL-2.0).

## Credits

Built with [FFmpeg](https://ffmpeg.org/), [ffmpeg-next](https://github.com/zmwangx/rust-ffmpeg), and [Tokio](https://tokio.rs/).
