/**
 * Integration-test harness for web/server.js.
 *
 * The server is spawned as a real child process rather than imported, because
 * the behaviours under test include startup refusal (process.exit) and the
 * WebSocket upgrade path, neither of which is reachable from an in-process
 * import of a module that listens at load time.
 *
 * Every server gets its own temp state dir (uploads/outputs/pid file) and its
 * own temp resources dir, so tests never touch the repository tree and can run
 * in parallel.
 *
 * This lives outside web/test/ on purpose: `node --test` default discovery
 * picks up every .js file under a directory named `test`, not just
 * *.test.js, so a harness kept there is loaded and reported as a test file.
 */
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, writeFile, rm, chmod } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = path.resolve(HERE, "..", "..");
const SERVER_ENTRY = path.join(REPO_ROOT, "web", "server.js");

/** Env vars that must never leak in from the developer's shell. */
const SCRUBBED = [
  "TRANSCODER_API_KEY",
  "TRANSCODER_ADMIN_KEY",
  "TRANSCODER_WS_AUTH_TIMEOUT_MS",
  "DESKTOP_MODE",
  "CORS_ORIGINS",
  "OBJECT_STORE_URL",
  "OBJECT_STORE_BUCKET",
  "CLUSTER_MASTER",
  "PORT",
];

async function makeDirs({ localMode, files }) {
  const stateDir = await mkdtemp(path.join(tmpdir(), "tx-state-"));
  const resourcesDir = await mkdtemp(path.join(tmpdir(), "tx-res-"));
  await mkdir(path.join(stateDir, "uploads"), { recursive: true });
  await mkdir(path.join(stateDir, "outputs"), { recursive: true });
  await mkdir(path.join(resourcesDir, "bin"), { recursive: true });

  if (localMode) {
    // A stub stands in for the Rust coordinator so local-mode behaviour is
    // deterministic whether or not the developer has built the binaries.
    const bin = path.join(resourcesDir, "bin", "transcoder-coordinator");
    await writeFile(bin, "#!/bin/sh\nexit 0\n");
    await chmod(bin, 0o755);
  }

  for (const [name, contents] of Object.entries(files)) {
    await writeFile(path.join(stateDir, "uploads", name), contents);
  }
  return { stateDir, resourcesDir };
}

function baseEnv(stateDir, resourcesDir, env) {
  const e = { ...process.env };
  for (const k of SCRUBBED) delete e[k];
  return {
    ...e,
    PORT: "0",
    TRANSCODER_STATE_DIR: stateDir,
    TRANSCODER_RESOURCES_DIR: resourcesDir,
    ...env,
  };
}

/**
 * Spawn the server and resolve once it reports its bound port.
 * @returns {Promise<{url:string, wsUrl:string, port:number, stateDir:string,
 *                    resourcesDir:string, child:import("node:child_process").ChildProcess,
 *                    stop:() => Promise<void>}>}
 */
export async function startServer({ env = {}, localMode = false, files = {} } = {}) {
  const { stateDir, resourcesDir } = await makeDirs({ localMode, files });
  const child = spawn(process.execPath, [SERVER_ENTRY], {
    cwd: REPO_ROOT,
    env: baseEnv(stateDir, resourcesDir, env),
    stdio: ["ignore", "pipe", "pipe"],
  });

  let out = "";
  let err = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");

  const cleanup = async () => {
    await rm(stateDir, { recursive: true, force: true });
    await rm(resourcesDir, { recursive: true, force: true });
  };

  const stop = async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = new Promise((r) => child.once("exit", r));
      child.kill("SIGTERM");
      await Promise.race([exited, new Promise((r) => setTimeout(() => { child.kill("SIGKILL"); r(); }, 5000))]);
    }
    await cleanup();
  };

  return await new Promise((resolve, reject) => {
    const timer = setTimeout(async () => {
      await stop();
      reject(new Error(`server did not report a port within 10s\n--- stdout ---\n${out}\n--- stderr ---\n${err}`));
    }, 10_000);

    const fail = async (e) => {
      clearTimeout(timer);
      await stop();
      reject(e);
    };

    child.stdout.on("data", (d) => {
      out += d;
      // Desktop mode reports its ephemeral port on a machine-readable marker
      // instead of the human log line, and binds 127.0.0.1.
      const m = out.match(/listening on http:\/\/localhost:(\d+)/) || out.match(/__DESKTOP_READY__PORT=(\d+)/);
      if (!m) return;
      clearTimeout(timer);
      const port = Number(m[1]);
      resolve({
        url: `http://127.0.0.1:${port}`,
        wsUrl: `ws://127.0.0.1:${port}/ws`,
        port,
        stateDir,
        resourcesDir,
        child,
        stdout: () => out,
        stderr: () => err,
        stop,
      });
    });
    child.stderr.on("data", (d) => { err += d; });
    child.once("error", fail);
    child.once("exit", (code) => {
      if (out.includes("listening on http") || out.includes("__DESKTOP_READY__PORT=")) return;
      fail(new Error(`server exited early with code ${code}\n--- stdout ---\n${out}\n--- stderr ---\n${err}`));
    });
  });
}

/**
 * Spawn the server expecting it to refuse to start, and resolve with how it
 * died. Used to prove the fail-closed startup check.
 */
export async function startServerExpectingExit({ env = {} } = {}) {
  const { stateDir, resourcesDir } = await makeDirs({ localMode: false, files: {} });
  const child = spawn(process.execPath, [SERVER_ENTRY], {
    cwd: REPO_ROOT,
    env: baseEnv(stateDir, resourcesDir, env),
    stdio: ["ignore", "pipe", "pipe"],
  });
  let out = "";
  let err = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (d) => { out += d; });
  child.stderr.on("data", (d) => { err += d; });

  const code = await new Promise((resolve) => {
    const timer = setTimeout(() => { child.kill("SIGKILL"); resolve("TIMEOUT"); }, 10_000);
    child.once("exit", (c) => { clearTimeout(timer); resolve(c); });
  });
  await rm(stateDir, { recursive: true, force: true });
  await rm(resourcesDir, { recursive: true, force: true });
  return { code, stdout: out, stderr: err };
}

/**
 * fetch() that never throws on non-2xx and exposes headers + parsed body.
 *
 * Every request is bounded. Without this, a route that hangs upstream (the
 * pre-fix url-import dials its target with a 300s socket timeout) keeps a
 * libuv handle alive and the whole test file never exits.
 */
export async function req(base, pathname, { timeoutMs = 15_000, ...opts } = {}) {
  let res;
  try {
    res = await fetch(base + pathname, { ...opts, signal: AbortSignal.timeout(timeoutMs) });
  } catch (e) {
    if (e?.name === "TimeoutError" || e?.name === "AbortError") {
      throw new Error(`request to ${pathname} did not complete within ${timeoutMs}ms`);
    }
    throw e;
  }
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* non-JSON is fine */ }
  return { status: res.status, headers: res.headers, text, json };
}
