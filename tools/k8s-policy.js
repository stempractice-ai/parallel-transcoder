// Static policy checks over rendered Kubernetes manifests.
//
// OKE virtual nodes, which pt1-cluster runs, reject or silently ignore pod-spec
// fields that ordinary nodes accept. The rules below encode Oracle's list
// (https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contengcomparingvirtualwithmanagednodes_topic.htm)
// and this project's hardening contract, so a manifest edit cannot quietly
// bring back a field that applies cleanly and protects nothing.
//
// Lives in tools/, not k8s/ (package.json extraResources ships k8s/ inside the
// desktop installer) and not in a test/ directory (node --test executes every
// .js file there).

import { execFileSync, spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadAll } from "js-yaml";

export const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// Every kubectl call here is offline: the subcommands are literals and
// KUBECONFIG points at an empty file, so no edit to this module can make it
// reach a cluster.
const KUBECTL_ENV = { ...process.env, KUBECONFIG: "/dev/null" };

export const KUBECTL_AVAILABLE =
  spawnSync("kubectl", ["version", "--client"], { env: KUBECTL_ENV, stdio: "ignore" }).status === 0;

// Renders a kustomization directory (relative to the repository root) into its
// list of objects.
export function render(dir) {
  const absDir = path.resolve(REPO_ROOT, dir);
  let out;
  try {
    out = execFileSync("kubectl", ["kustomize", absDir], {
      env: KUBECTL_ENV,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      maxBuffer: 16 * 1024 * 1024,
    });
  } catch (err) {
    const stderr = err.stderr ? String(err.stderr).trim() : "";
    throw new Error(`kubectl kustomize ${dir} failed: ${stderr || err.message}`);
  }
  return loadAll(out).filter((doc) => doc != null);
}

const WORKLOAD_KINDS = new Set(["Deployment", "StatefulSet", "Job"]);

export function podTemplates(docs) {
  return docs
    .filter((doc) => WORKLOAD_KINDS.has(doc.kind))
    .map((doc) => ({
      kind: doc.kind,
      name: doc.metadata?.name,
      namespace: doc.metadata?.namespace,
      labels: doc.spec?.template?.metadata?.labels ?? {},
      spec: doc.spec?.template?.spec ?? {},
    }));
}

// Virtual-node restrictions a pod template can violate, one rule per Oracle
// restriction:
//
//   vn/multiple-emptydir         more than one emptyDir volume
//   vn/emptydir-options          emptyDir sizeLimit, or medium other than "" / "Memory"
//   vn/pod-security-field        an unsupported pod securityContext field
//   vn/container-security-field  privileged, allowPrivilegeEscalation: true, procMount
//   vn/requests-ne-limits        cpu/memory request missing or different from its limit
//   vn/host-namespace            hostNetwork, hostIPC, hostPID, shareProcessNamespace
//   vn/hostpath                  a hostPath volume
//   vn/hostport                  a container port with hostPort or hostIP
//   vn/init-containers           any init container
//   vn/grpc-probe                a gRPC liveness/readiness/startup probe
//
// terminationGracePeriodSeconds is also on Oracle's list but is deliberately
// NOT a rule: production renders it today and the pods run, so the virtual
// kubelet ignores it rather than rejecting it. Removing it is outside Wave 2.
const VN_POD_SECURITY_FIELDS = [
  "seccompProfile",
  "fsGroup",
  "fsGroupChangePolicy",
  "supplementalGroups",
  "seLinuxOptions",
  "sysctls",
];
const VN_HOST_FIELDS = ["hostNetwork", "hostIPC", "hostPID", "shareProcessNamespace"];
const PROBES = ["livenessProbe", "readinessProbe", "startupProbe"];

export function virtualNodeViolations(spec) {
  const out = [];
  const flag = (rule, detail) => out.push({ rule, detail });
  const volumes = spec.volumes ?? [];
  const emptyDirs = volumes.filter((v) => v.emptyDir != null);

  if (emptyDirs.length > 1) flag("vn/multiple-emptydir", emptyDirs.map((v) => v.name).join(", "));
  for (const v of emptyDirs) {
    if (v.emptyDir.sizeLimit !== undefined) flag("vn/emptydir-options", `${v.name}: sizeLimit`);
    const medium = v.emptyDir.medium ?? "";
    if (medium !== "" && medium !== "Memory") flag("vn/emptydir-options", `${v.name}: medium ${medium}`);
  }
  for (const v of volumes) {
    if (v.hostPath != null) flag("vn/hostpath", v.name);
  }

  const podSecurity = spec.securityContext ?? {};
  for (const field of VN_POD_SECURITY_FIELDS) {
    if (podSecurity[field] !== undefined) flag("vn/pod-security-field", field);
  }
  for (const field of VN_HOST_FIELDS) {
    if (spec[field] === true) flag("vn/host-namespace", field);
  }
  if ((spec.initContainers ?? []).length > 0) {
    flag("vn/init-containers", spec.initContainers.map((c) => c.name).join(", "));
  }

  for (const c of spec.containers ?? []) {
    const security = c.securityContext ?? {};
    // Oracle lists `privileged` unqualified, unlike `allowPrivilegeEscalation:
    // true`, and the field is a pointer, so even an explicit `false` reaches the
    // virtual kubelet. Leave it unset.
    if (security.privileged !== undefined) flag("vn/container-security-field", `${c.name}: privileged`);
    if (security.allowPrivilegeEscalation === true) {
      flag("vn/container-security-field", `${c.name}: allowPrivilegeEscalation: true`);
    }
    if (security.procMount !== undefined) flag("vn/container-security-field", `${c.name}: procMount`);

    // Compared as written, not as parsed quantities: "1" and "1000m" are equal
    // to Kubernetes but not to a reader checking that requests match limits.
    const requests = c.resources?.requests ?? {};
    const limits = c.resources?.limits ?? {};
    for (const resource of ["cpu", "memory"]) {
      const request = requests[resource];
      const limit = limits[resource];
      if (request === undefined || limit === undefined || String(request) !== String(limit)) {
        flag("vn/requests-ne-limits", `${c.name}: ${resource} request ${request} vs limit ${limit}`);
      }
    }

    for (const p of c.ports ?? []) {
      if (p.hostPort !== undefined || p.hostIP !== undefined) {
        flag("vn/hostport", `${c.name}: ${p.name ?? p.containerPort}`);
      }
    }
    for (const probe of PROBES) {
      if (c[probe]?.grpc != null) flag("vn/grpc-probe", `${c.name}: ${probe}`);
    }
  }
  return out;
}

// The uid both images already run as (Dockerfile, Dockerfile.web), so pinning
// it changes no file ownership.
const APP_UID = 10001;

// The hardening contract for this project's own workloads:
//
//   hd/run-as-non-root      pod runAsNonRoot is not true
//   hd/run-as-user          pod runAsUser is not the image's uid
//   hd/privilege-escalation container allowPrivilegeEscalation is not false
//   hd/readonly-root        container readOnlyRootFilesystem is not true
//   hd/drop-all             container capabilities.drop lacks "ALL"
//   hd/no-writable-mount    read-only root but no writable emptyDir mount
//   hd/sa-token             service-account token mounted into a pod that does
//                           not call the API server
//
// Only fields virtual nodes honour; seccompProfile belongs to the managed-node
// component (k8s/components/managed-node-hardening).
export function hardeningViolations(spec, { apiServerAccess = false } = {}) {
  const out = [];
  const flag = (rule, detail) => out.push({ rule, detail });
  const podSecurity = spec.securityContext ?? {};
  if (podSecurity.runAsNonRoot !== true) flag("hd/run-as-non-root", `runAsNonRoot is ${podSecurity.runAsNonRoot}`);
  if (podSecurity.runAsUser !== APP_UID) flag("hd/run-as-user", `runAsUser is ${podSecurity.runAsUser}`);

  const emptyDirs = new Set((spec.volumes ?? []).filter((v) => v.emptyDir != null).map((v) => v.name));
  for (const c of spec.containers ?? []) {
    const security = c.securityContext ?? {};
    if (security.allowPrivilegeEscalation !== false) flag("hd/privilege-escalation", c.name);
    if (security.readOnlyRootFilesystem !== true) flag("hd/readonly-root", c.name);
    if (!(security.capabilities?.drop ?? []).includes("ALL")) flag("hd/drop-all", c.name);
    const writable = (c.volumeMounts ?? []).some((m) => emptyDirs.has(m.name) && m.readOnly !== true);
    if (security.readOnlyRootFilesystem === true && !writable) flag("hd/no-writable-mount", c.name);
  }
  if (!apiServerAccess && spec.automountServiceAccountToken !== false) {
    flag("hd/sa-token", `automountServiceAccountToken is ${spec.automountServiceAccountToken}`);
  }
  return out;
}

function selects(selector, labels) {
  if (selector.matchExpressions?.length) {
    throw new Error("matchExpressions is not supported by this checker; extend selects() before using it");
  }
  return Object.entries(selector.matchLabels ?? {}).every(([key, value]) => labels[key] === value);
}

function declaresPort(spec, port, protocol) {
  return (spec.containers ?? []).some((c) =>
    (c.ports ?? []).some(
      (p) =>
        (p.protocol ?? "TCP") === protocol && (typeof port === "string" ? p.name === port : p.containerPort === port),
    ),
  );
}

// Findings for NetworkPolicy objects, judged against the pod templates in the
// same render:
//
//   np/selects-nothing       a non-empty podSelector matches no pod template in its namespace
//   np/from-selects-nothing  an ingress from[].podSelector matches no pod template
//   np/port-not-declared     an allowed port is not a containerPort of any selected pod
//   np/udp-port              an ingress rule allows UDP (the SRT data plane has never worked)
export function networkPolicyFindings(docs) {
  const out = [];
  const flag = (rule, detail) => out.push({ rule, detail });
  const templates = podTemplates(docs);

  for (const policy of docs.filter((doc) => doc.kind === "NetworkPolicy")) {
    const name = policy.metadata?.name;
    const local = templates.filter((t) => t.namespace === policy.metadata?.namespace);
    const podSelector = policy.spec?.podSelector ?? {};
    const selected = local.filter((t) => selects(podSelector, t.labels));
    if (Object.keys(podSelector.matchLabels ?? {}).length > 0 && selected.length === 0) {
      flag("np/selects-nothing", name);
    }

    for (const [i, rule] of (policy.spec?.ingress ?? []).entries()) {
      for (const peer of rule.from ?? []) {
        // A podSelector alone means pods in the policy's own namespace. Paired
        // with a namespaceSelector it reaches namespaces this render cannot
        // see, so it is not judged.
        if (peer.podSelector && !peer.namespaceSelector && !local.some((t) => selects(peer.podSelector, t.labels))) {
          flag("np/from-selects-nothing", `${name}: ingress[${i}] ${JSON.stringify(peer.podSelector)}`);
        }
      }
      for (const port of rule.ports ?? []) {
        const protocol = port.protocol ?? "TCP";
        if (protocol === "UDP") flag("np/udp-port", `${name}: ${port.port ?? "all"}/UDP`);
        // A policy that selects nothing is already reported; its ports are moot.
        if (port.port !== undefined && selected.length > 0 && !selected.some((t) => declaresPort(t.spec, port.port, protocol))) {
          flag("np/port-not-declared", `${name}: ${port.port}/${protocol}`);
        }
      }
    }
  }
  return out;
}
