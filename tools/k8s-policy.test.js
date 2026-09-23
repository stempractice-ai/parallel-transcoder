// Manifest policy tests. The rule tests prove every rule can fire on its own;
// the rendered-manifest tests run the rules over the real overlays, the
// managed-node fixture and the read-only-root probe.
import { describe, test, before } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { loadAll } from "js-yaml";

import {
  KUBECTL_AVAILABLE,
  REPO_ROOT,
  render,
  podTemplates,
  virtualNodeViolations,
  hardeningViolations,
  networkPolicyFindings,
} from "./k8s-policy.js";

const OCI = "k8s/overlays/oci-a1";
const KIND = "k8s/overlays/kind";
const FIXTURE = "k8s/fixtures/oci-a1-managed-nodes";
const PROBE = "k8s/probes/readonly-root-probe.yaml";

function expectOnly(found, rule, label) {
  assert.deepEqual(
    found.map((f) => f.rule),
    [rule],
    `${label}: ${JSON.stringify(found)}`,
  );
}

// { "Kind/name": [findings] } for every template with at least one finding.
function collect(templates, check) {
  return Object.fromEntries(
    templates.map((t) => [`${t.kind}/${t.name}`, check(t)]).filter(([, found]) => found.length > 0),
  );
}

// Virtual-node compliant and fully hardened. Each rule test changes exactly one
// thing and expects exactly that rule back.
function goodPod() {
  return {
    automountServiceAccountToken: false,
    securityContext: { runAsNonRoot: true, runAsUser: 10001 },
    containers: [
      {
        name: "app",
        image: "example:v1.0.0",
        securityContext: {
          allowPrivilegeEscalation: false,
          readOnlyRootFilesystem: true,
          capabilities: { drop: ["ALL"] },
        },
        resources: { requests: { cpu: "1", memory: "2Gi" }, limits: { cpu: "1", memory: "2Gi" } },
        ports: [{ name: "http", containerPort: 3000, protocol: "TCP" }],
        readinessProbe: { httpGet: { path: "/api/ready", port: 3000 } },
        volumeMounts: [{ name: "scratch", mountPath: "/scratch" }],
      },
    ],
    volumes: [{ name: "scratch", emptyDir: {} }],
  };
}

test("the reference pod passes both rule sets", () => {
  assert.deepEqual(virtualNodeViolations(goodPod()), []);
  assert.deepEqual(hardeningViolations(goodPod(), { apiServerAccess: false }), []);
});

const VIRTUAL_NODE_CASES = {
  "vn/multiple-emptydir": [["a second emptyDir", (p) => p.volumes.push({ name: "cache", emptyDir: {} })]],
  "vn/emptydir-options": [
    ["sizeLimit", (p) => { p.volumes[0].emptyDir.sizeLimit = "1Gi"; }],
    ["medium HugePages", (p) => { p.volumes[0].emptyDir.medium = "HugePages"; }],
  ],
  "vn/pod-security-field": [
    ["seccompProfile", (p) => { p.securityContext.seccompProfile = { type: "RuntimeDefault" }; }],
    ["fsGroup", (p) => { p.securityContext.fsGroup = 10001; }],
    ["fsGroupChangePolicy", (p) => { p.securityContext.fsGroupChangePolicy = "OnRootMismatch"; }],
    ["supplementalGroups", (p) => { p.securityContext.supplementalGroups = [10001]; }],
    ["seLinuxOptions", (p) => { p.securityContext.seLinuxOptions = { type: "spc_t" }; }],
    ["sysctls", (p) => { p.securityContext.sysctls = [{ name: "net.core.somaxconn", value: "1024" }]; }],
  ],
  "vn/container-security-field": [
    ["privileged, even when false", (_p, c) => { c.securityContext.privileged = false; }],
    ["allowPrivilegeEscalation: true", (_p, c) => { c.securityContext.allowPrivilegeEscalation = true; }],
    ["procMount", (_p, c) => { c.securityContext.procMount = "Unmasked"; }],
  ],
  "vn/requests-ne-limits": [
    ["cpu limit above request", (_p, c) => { c.resources.limits.cpu = "2"; }],
    ["memory request missing", (_p, c) => { delete c.resources.requests.memory; }],
  ],
  "vn/host-namespace": [
    ["hostNetwork", (p) => { p.hostNetwork = true; }],
    ["hostIPC", (p) => { p.hostIPC = true; }],
    ["hostPID", (p) => { p.hostPID = true; }],
    ["shareProcessNamespace", (p) => { p.shareProcessNamespace = true; }],
  ],
  "vn/hostpath": [["a hostPath volume", (p) => p.volumes.push({ name: "host", hostPath: { path: "/data" } })]],
  "vn/hostport": [
    ["hostPort", (_p, c) => { c.ports[0].hostPort = 80; }],
    ["hostIP", (_p, c) => { c.ports[0].hostIP = "0.0.0.0"; }],
  ],
  "vn/init-containers": [
    ["an init container", (p) => { p.initContainers = [{ name: "init", image: "example:v1.0.0" }]; }],
  ],
  "vn/grpc-probe": [
    ["livenessProbe", (_p, c) => { c.livenessProbe = { grpc: { port: 3000 } }; }],
    ["readinessProbe", (_p, c) => { c.readinessProbe = { grpc: { port: 3000 } }; }],
    ["startupProbe", (_p, c) => { c.startupProbe = { grpc: { port: 3000 } }; }],
  ],
  "vn/volume-mode": [
    ["secret defaultMode 0400 as js-yaml reads a raw file (400)", (p) => p.volumes.push({ name: "key", secret: { secretName: "s", defaultMode: 400 } })],
    ["secret item mode 0400 written in decimal", (p) => p.volumes.push({ name: "key", secret: { secretName: "s", items: [{ key: "k", path: "k", mode: 256 }] } })],
    ["configMap defaultMode 0600", (p) => p.volumes.push({ name: "cfg", configMap: { name: "c", defaultMode: 384 } })],
    ["projected secret item mode 0400", (p) => p.volumes.push({ name: "proj", projected: { sources: [{ secret: { name: "s", items: [{ key: "k", path: "k", mode: 256 }] } }] } })],
  ],
};

for (const [rule, cases] of Object.entries(VIRTUAL_NODE_CASES)) {
  test(`virtual-node rule ${rule} fires on its own`, () => {
    for (const [label, mutate] of cases) {
      const pod = goodPod();
      mutate(pod, pod.containers[0]);
      expectOnly(virtualNodeViolations(pod), rule, label);
    }
  });
}

test("a Memory-backed emptyDir is allowed on virtual nodes", () => {
  const pod = goodPod();
  pod.volumes[0].emptyDir.medium = "Memory";
  assert.deepEqual(virtualNodeViolations(pod), []);
});

test("a 0644 volume mode is allowed on virtual nodes", () => {
  const pod = goodPod();
  pod.volumes.push(
    { name: "key", secret: { secretName: "s", defaultMode: 420 } },
    { name: "cfg", configMap: { name: "c", items: [{ key: "k", path: "k", mode: 420 }] } },
  );
  assert.deepEqual(virtualNodeViolations(pod), []);
});

const HARDENING_CASES = {
  "hd/run-as-non-root": [["runAsNonRoot unset", (p) => { delete p.securityContext.runAsNonRoot; }]],
  "hd/run-as-user": [["a uid other than the image's", (p) => { p.securityContext.runAsUser = 1000; }]],
  "hd/privilege-escalation": [
    ["allowPrivilegeEscalation unset", (_p, c) => { delete c.securityContext.allowPrivilegeEscalation; }],
  ],
  "hd/readonly-root": [["writable root", (_p, c) => { c.securityContext.readOnlyRootFilesystem = false; }]],
  "hd/drop-all": [["drops less than ALL", (_p, c) => { c.securityContext.capabilities.drop = ["NET_RAW"]; }]],
  "hd/no-writable-mount": [
    ["no emptyDir mounted", (_p, c) => { c.volumeMounts = []; }],
    ["emptyDir mounted read-only", (_p, c) => { c.volumeMounts[0].readOnly = true; }],
  ],
  "hd/sa-token": [["token left at its default", (p) => { delete p.automountServiceAccountToken; }]],
};

for (const [rule, cases] of Object.entries(HARDENING_CASES)) {
  test(`hardening rule ${rule} fires on its own`, () => {
    for (const [label, mutate] of cases) {
      const pod = goodPod();
      mutate(pod, pod.containers[0]);
      expectOnly(hardeningViolations(pod, { apiServerAccess: false }), rule, label);
    }
  });
}

test("a pod that calls the API server may keep its service-account token", () => {
  const pod = goodPod();
  delete pod.automountServiceAccountToken;
  assert.deepEqual(hardeningViolations(pod, { apiServerAccess: true }), []);
});

function workload(kind, name, labels, ports) {
  return {
    kind,
    metadata: { name, namespace: "app" },
    spec: { template: { metadata: { labels }, spec: { containers: [{ name, ports }] } } },
  };
}

// Two workloads, a default deny, and one allow rule that is correct.
function goodPolicyDocs() {
  return [
    workload("Deployment", "web", { app: "web" }, [{ name: "http", containerPort: 3000, protocol: "TCP" }]),
    workload("StatefulSet", "node", { app: "node" }, [
      { name: "cluster-ws", containerPort: 9900, protocol: "TCP" },
      { name: "srt", containerPort: 9910, protocol: "UDP" },
    ]),
    {
      kind: "NetworkPolicy",
      metadata: { name: "deny", namespace: "app" },
      spec: { podSelector: {}, policyTypes: ["Ingress"] },
    },
    {
      kind: "NetworkPolicy",
      metadata: { name: "node", namespace: "app" },
      spec: {
        podSelector: { matchLabels: { app: "node" } },
        policyTypes: ["Ingress"],
        ingress: [{ from: [{ podSelector: { matchLabels: { app: "web" } } }], ports: [{ protocol: "TCP", port: 9900 }] }],
      },
    },
  ];
}

function withNodePolicy(mutate) {
  const docs = goodPolicyDocs();
  mutate(docs.find((d) => d.kind === "NetworkPolicy" && d.metadata.name === "node").spec);
  return docs;
}

test("the reference policies produce no findings", () => {
  assert.deepEqual(networkPolicyFindings(goodPolicyDocs()), []);
});

const POLICY_CASES = {
  "np/selects-nothing": [["podSelector matches no workload", (s) => { s.podSelector = { matchLabels: { app: "gone" } }; }]],
  "np/from-selects-nothing": [
    ["from podSelector matches no workload", (s) => { s.ingress[0].from[0].podSelector = { matchLabels: { app: "gone" } }; }],
  ],
  "np/port-not-declared": [
    ["undeclared port number", (s) => { s.ingress[0].ports[0].port = 9999; }],
    ["undeclared port name", (s) => { s.ingress[0].ports[0].port = "gone"; }],
    ["port declared only for UDP", (s) => { s.ingress[0].ports[0].port = 9910; }],
  ],
  "np/udp-port": [["a UDP port", (s) => { s.ingress[0].ports.push({ protocol: "UDP", port: 9910 }); }]],
};

for (const [rule, cases] of Object.entries(POLICY_CASES)) {
  test(`network-policy rule ${rule} fires on its own`, () => {
    for (const [label, mutate] of cases) {
      expectOnly(networkPolicyFindings(withNodePolicy(mutate)), rule, label);
    }
  });
}

test("a named policy port resolves to the container port that declares it", () => {
  const docs = withNodePolicy((s) => { s.ingress[0].ports[0].port = "cluster-ws"; });
  assert.deepEqual(networkPolicyFindings(docs), []);
});

test("matchExpressions selectors are refused rather than misjudged", () => {
  const docs = withNodePolicy((s) => {
    s.podSelector = { matchExpressions: [{ key: "app", operator: "In", values: ["node"] }] };
  });
  assert.throws(() => networkPolicyFindings(docs), /matchExpressions/);
});

// Needs no kubectl: the probe is a plain file that no kustomization includes.
test("the read-only-root probe Job schedules on virtual nodes, is hardened, and expires", () => {
  const docs = loadAll(fs.readFileSync(path.join(REPO_ROOT, PROBE), "utf8")).filter((d) => d != null);
  const templates = podTemplates(docs);
  assert.deepEqual(templates.map((t) => t.kind), ["Job"]);
  assert.deepEqual(virtualNodeViolations(templates[0].spec), []);
  assert.deepEqual(hardeningViolations(templates[0].spec, { apiServerAccess: false }), []);
  // Virtual-node Jobs bill until deleted; the TTL is the backstop for a
  // forgotten `kubectl delete job`.
  const ttl = docs.find((d) => d.kind === "Job").spec.ttlSecondsAfterFinished;
  assert.ok(Number.isInteger(ttl) && ttl > 0, `ttlSecondsAfterFinished is ${ttl}`);
});

function versionAtLeast(version, floor) {
  for (let i = 0; i < floor.length; i++) {
    if (version[i] !== floor[i]) return version[i] > floor[i];
  }
  return true;
}

const SKIP_RENDERED =
  !KUBECTL_AVAILABLE && !process.env.CI
    ? "kubectl not found on PATH: rendered-manifest checks skipped (with CI set they fail instead)"
    : false;

describe("rendered manifests", { skip: SKIP_RENDERED }, () => {
  const cache = new Map();
  const rendered = (dir) => {
    if (!cache.has(dir)) cache.set(dir, render(dir));
    return cache.get(dir);
  };
  const template = (dir, name) => {
    const found = podTemplates(rendered(dir)).find((t) => t.name === name);
    assert.ok(found, `${dir} renders ${name}`);
    return found;
  };
  const container = (t, name) => {
    const found = (t.spec.containers ?? []).find((c) => c.name === name);
    assert.ok(found, `${t.name} has a ${name} container`);
    return found;
  };
  const TRANSCODER_WORKLOADS = [
    "Deployment/transcoder-web",
    "Deployment/transcoder-worker-cpu",
    "StatefulSet/transcoder-node",
  ];

  before(() => {
    assert.ok(KUBECTL_AVAILABLE, "kubectl must be on PATH when CI is set: these checks render the overlays");
  });

  test("oci-a1 renders exactly the three transcoder workloads", () => {
    const names = podTemplates(rendered(OCI)).map((t) => `${t.kind}/${t.name}`).sort();
    assert.deepEqual(names, TRANSCODER_WORKLOADS);
  });

  test("oci-a1 workloads use no field OKE virtual nodes reject or ignore", () => {
    assert.deepEqual(collect(podTemplates(rendered(OCI)), (t) => virtualNodeViolations(t.spec)), {});
  });

  test("oci-a1 workloads are hardened, and only the web pod keeps its API token", () => {
    const found = collect(podTemplates(rendered(OCI)), (t) =>
      hardeningViolations(t.spec, { apiServerAccess: t.name === "transcoder-web" }),
    );
    assert.deepEqual(found, {});
  });

  test("oci-a1 web points TRANSCODER_STATE_DIR at its single emptyDir", () => {
    const web = template(OCI, "transcoder-web");
    const emptyDirs = (web.spec.volumes ?? []).filter((v) => v.emptyDir != null);
    assert.equal(emptyDirs.length, 1, "the web pod declares exactly one emptyDir");
    const c = container(web, "transcoder-web");
    const mount = (c.volumeMounts ?? []).find((m) => m.name === emptyDirs[0].name);
    assert.ok(mount, "the emptyDir is mounted into the web container");
    const stateDir = (c.env ?? []).find((e) => e.name === "TRANSCODER_STATE_DIR")?.value;
    assert.equal(stateDir, mount.mountPath);
  });

  test("oci-a1 renders no NetworkPolicy, which virtual nodes would accept and not enforce", () => {
    const policies = rendered(OCI).filter((d) => d.kind === "NetworkPolicy").map((d) => d.metadata.name);
    assert.deepEqual(policies, []);
  });

  test("oci-a1 web image is v0.1.16 or later, the first with TRANSCODER_STATE_DIR", () => {
    // Read from the render, not kustomization.yaml, so an images: override
    // cannot slip past it.
    const { image } = container(template(OCI, "transcoder-web"), "transcoder-web");
    const match = /:v(\d+)\.(\d+)\.(\d+)$/.exec(image);
    assert.ok(match, `web image tag is not vMAJOR.MINOR.PATCH: ${image}`);
    assert.ok(versionAtLeast(match.slice(1).map(Number), [0, 1, 16]), `${image} predates TRANSCODER_STATE_DIR`);
  });

  test("kind transcoder-node StatefulSet is hardened", () => {
    const node = template(KIND, "transcoder-node");
    assert.equal(node.kind, "StatefulSet");
    assert.deepEqual(hardeningViolations(node.spec, { apiServerAccess: false }), []);
  });

  test("managed-node fixture: a default deny plus policies that select real pods on declared ports", () => {
    const docs = rendered(FIXTURE);
    const defaultDeny = docs.some(
      (d) =>
        d.kind === "NetworkPolicy" &&
        Object.keys(d.spec.podSelector ?? {}).length === 0 &&
        (d.spec.policyTypes ?? []).includes("Ingress") &&
        (d.spec.ingress ?? []).length === 0,
    );
    assert.ok(defaultDeny, "a default-deny ingress policy selects every pod");
    assert.deepEqual(networkPolicyFindings(docs), []);
  });

  test("managed-node fixture: web admits TCP 3000 from the load-balancer subnet", () => {
    // The OCI Native Ingress Controller only programs the load balancer; the
    // traffic itself arrives from the LB subnet, not the controller namespace.
    const admits = rendered(FIXTURE)
      .filter((d) => d.kind === "NetworkPolicy")
      .filter((d) => d.spec.podSelector?.matchLabels?.["app.kubernetes.io/name"] === "transcoder-web")
      .some((d) =>
        (d.spec.ingress ?? []).some(
          (rule) =>
            (rule.from ?? []).some((peer) => peer.ipBlock?.cidr === "10.0.20.0/24") &&
            (rule.ports ?? []).some((p) => (p.protocol ?? "TCP") === "TCP" && p.port === 3000),
        ),
      );
    assert.ok(admits);
  });

  test("managed-node fixture: every NetworkPolicy is in the transcoder namespace", () => {
    const namespaces = rendered(FIXTURE)
      .filter((d) => d.kind === "NetworkPolicy")
      .map((d) => d.metadata.namespace);
    assert.ok(namespaces.length > 0, "the fixture renders NetworkPolicies");
    assert.deepEqual([...new Set(namespaces)], ["transcoder"]);
  });

  test("managed-node fixture: every workload runs the RuntimeDefault seccomp profile", () => {
    const templates = podTemplates(rendered(FIXTURE));
    assert.deepEqual(templates.map((t) => `${t.kind}/${t.name}`).sort(), TRANSCODER_WORKLOADS);
    const missing = templates
      .filter((t) => t.spec.securityContext?.seccompProfile?.type !== "RuntimeDefault")
      .map((t) => t.name);
    assert.deepEqual(missing, []);
  });
});
