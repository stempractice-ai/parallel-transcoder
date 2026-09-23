# Kubernetes deployment

StatefulSet-based deployment of the `transcoder-node` cluster daemon.

## Layout

```
k8s/
├── base/              # StatefulSet + headless Service + master Service +
│                      # ConfigMap + ServiceAccount — no assumptions about
│                      # node pools, GPU, or object storage.
├── overlays/
│   ├── kind/          # Local dev on kind: in-cluster MinIO, CPU-only
│   │                  # encoder, tight resource limits.
│   ├── cloud/         # EKS/GKE: NVIDIA + VAAPI + AVX-512 node pools
│   │                  # selected via NFD labels, S3 via IRSA / Workload Identity.
│   └── oci-a1/        # OKE virtual nodes (pt1-cluster): master, CPU workers,
│                      # web UI behind the OCI Native Ingress Controller.
├── components/
│   └── managed-node-hardening/  # NetworkPolicy + seccomp. Managed nodes only;
│                                # no overlay includes it.
├── fixtures/
│   └── oci-a1-managed-nodes/    # Test-only render: oci-a1 + that component.
└── probes/
    └── readonly-root-probe.yaml # One-shot Job for the Wave 2 runbook below.
```

## Kind workflow

```bash
# 1. Create a multi-node kind cluster.
kind create cluster --name transcoder --config k8s/overlays/kind/kind-cluster.yaml

# 2. Build the container image for the right architecture.
docker build -t transcoder-node:dev .

# 3. Load it into every kind node (kind uses its own container runtime).
kind load docker-image transcoder-node:dev --name transcoder

# 4. Apply the overlay.
kubectl apply -k k8s/overlays/kind

# 5. Watch the cluster come up.
kubectl -n transcoder get pods -w
```

Pod-0 bootstraps as deterministic master (`--k8s-mode` short-circuits the
bully election); pods 1..N join `transcoder-node-0.transcoder`. SIGTERM
triggers a clean `NodeLeave` broadcast before K8s kills the pod.

## Submitting a job

The coordinator submits jobs through the `transcoder-master` Service, which
endpoints to whichever pod has `statefulset.kubernetes.io/pod-name=transcoder-node-0`:

```bash
kubectl -n transcoder port-forward svc/transcoder-master 9900:9900
./bin/transcoder-coordinator --input video.mp4 --cluster --cluster-master 127.0.0.1:9900
```

## Encoder selection

`transcoder-node` picks its default encoder from env vars (see
`cluster/src/node.rs::detect_encoder_from_env`):

| `ENCODER_GPU` | `ENCODER_CPU_FEATURE` | Encoder chosen |
|---------------|-----------------------|----------------|
| `nvidia`      | —                     | `hevc_nvenc`   |
| `vaapi`       | —                     | `hevc_vaapi`   |
| `none`        | `avx512`              | `libsvtav1`    |
| `none`        | `baseline`            | `libx264`      |

The cloud overlay sets these via nodeAffinity onto labeled node pools; kind
hardcodes `none` / `baseline`.

## OCI overlay (`overlays/oci-a1`)

### Secrets the web pod needs before it will start

`transcoder-web` exits immediately without `TRANSCODER_API_KEY`, so create the Secret **before**
applying the overlay — applying first crash-loops the Deployment.

```bash
kubectl -n transcoder create secret generic transcoder-web-auth \
  --from-literal=api-key=$(openssl rand -hex 32) \
  --from-literal=admin-key=$(openssl rand -hex 32)
```

The two Secret keys map to the server's environment in `web-frontend.yaml`:
`api-key` → `TRANSCODER_API_KEY` (every client), `admin-key` → `TRANSCODER_ADMIN_KEY`
(worker scaling and bulk job deletion only).

Read a key back to hand to an operator (it is a shared key, the same for everyone):

```bash
kubectl -n transcoder get secret transcoder-web-auth -o jsonpath='{.data.api-key}' | base64 -d
```

Rotate — replace the Secret in place, then restart so the pod re-reads it:

```bash
kubectl -n transcoder create secret generic transcoder-web-auth \
  --from-literal=api-key=$(openssl rand -hex 32) \
  --from-literal=admin-key=$(openssl rand -hex 32) \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n transcoder rollout restart deploy/transcoder-web
```

Never put either value in a manifest, a ConfigMap, or a commit. The admin key is the one that can
resize the billable virtual-node pool — give it only to whoever is allowed to spend that money.

### Release order

```bash
docker buildx build --platform linux/arm64 -f Dockerfile.web \
  -t iad.ocir.io/idr5qsmifndm/transcoder-web:<tag> --push .
git tag web-<tag>
# create or rotate transcoder-web-auth (above) — must exist before the rollout
kubectl apply -k k8s/overlays/oci-a1
kubectl -n transcoder rollout status deploy/transcoder-web
```

The probes point at `/api/ready` (readiness) and `/api/health` (liveness). Both are exempt from key
authentication because the kubelet cannot send headers.

Every step above changes production and is run by the cluster owner, after written approval — not by
whoever wrote the code.

### Applying the Wave 2 posture

Every command below changes or reads production. The cluster owner runs them, after written
approval.

The manifests run every transcoder container as uid 10001 with a read-only root filesystem, no
privilege escalation and no Linux capabilities, and mount no service-account token into the node
pods. The web pod keeps uploads and outputs on its single `emptyDir` (`TRANSCODER_STATE_DIR`). Only
fields OKE virtual nodes honour are used: NetworkPolicy and seccomp live in
`components/managed-node-hardening`, which no overlay includes, because virtual nodes do not
support them.

Apply while no transcode job is running: the master pod restarts.

```bash
# Prerequisite: transcoder-web v0.1.16 is built and pushed (see Release order).
# These manifests must never run with an older web image: it writes a pid file
# into the now read-only root and crash-loops.
kubectl diff -k k8s/overlays/oci-a1                       # preview
kubectl apply -k k8s/overlays/oci-a1
kubectl -n transcoder rollout status statefulset/transcoder-node
kubectl -n transcoder rollout status deploy/transcoder-web
# No read-only-filesystem errors. Virtual nodes support `logs`, not `logs -f`.
for p in $(kubectl -n transcoder get pods -o name); do
  kubectl -n transcoder logs "$p" | grep -iE 'EROFS|read-only file system' && echo "^^ $p"
done
# The fields were accepted and persisted on the pod object
kubectl -n transcoder get pod -l app.kubernetes.io/name=transcoder-web \
  -o jsonpath='{.items[0].spec.containers[0].securityContext}'
# Enforcement, not just acceptance
kubectl apply -f k8s/probes/readonly-root-probe.yaml
kubectl -n transcoder wait --for=condition=complete job/readonly-root-probe --timeout=180s
kubectl -n transcoder logs job/readonly-root-probe        # expect ROOT_READONLY and SCRATCH_WRITABLE
kubectl -n transcoder delete job readonly-root-probe     # bills until deleted
```

Delete the probe Job even if the wait times out: `ttlSecondsAfterFinished` only removes a Job that
finished. `kubectl exec` and `port-forward` are not available on virtual nodes, which is why the
check is a Job.

Reading the probe:

- `ROOT_READONLY` and `SCRATCH_WRITABLE`: the control is enforced.
- `ROOT_WRITABLE`: virtual nodes accept `readOnlyRootFilesystem` but do not enforce it. Record
  that, leave the field (it is harmless), and stop claiming the control on this cluster.
- `SCRATCH_NOT_WRITABLE`: uid 10001 cannot write its `emptyDir`, so the daemons cannot work. Roll
  back.

Rollback:

```bash
kubectl -n transcoder rollout undo deploy/transcoder-web
kubectl -n transcoder rollout undo statefulset/transcoder-node
kubectl -n transcoder rollout undo deploy/transcoder-worker-cpu
```

`rollout undo` restores the previous pod templates in the cluster only; the next
`kubectl apply -k` reinstates these manifests unless the commit is reverted too. Rolling
`transcoder-web` back below v0.1.16 means rolling these manifests back with it.
