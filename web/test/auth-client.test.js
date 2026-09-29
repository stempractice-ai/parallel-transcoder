import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const SOURCE = fs.readFileSync(new URL("../public/auth.js", import.meta.url), "utf8");

function memoryStorage() {
  const m = new Map();
  return { getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), removeItem: (k) => m.delete(k) };
}

// Loads auth.js the way the browser does: a classic script that attaches Auth to window.
function loadAuth() {
  const window = { localStorage: memoryStorage(), sessionStorage: memoryStorage() };
  vm.runInContext(SOURCE, vm.createContext({ window, localStorage: window.localStorage, sessionStorage: window.sessionStorage }));
  const calls = [];
  window.Auth.onUnauthorized = (hadKey) => calls.push(hadKey);
  return { Auth: window.Auth, calls, window };
}

// Objects built inside the vm context carry its Object.prototype; copy them into
// this realm so deepStrictEqual compares contents only.
const plain = (o) => ({ ...o });

test("a 401 for a request sent before the key was entered keeps the new key", () => {
  const { Auth, calls } = loadAuth();
  Auth.set(Auth.KEY_ITEM, "new-key");
  Auth.rejected("");
  assert.equal(Auth.key(), "new-key");
  assert.deepEqual(calls, []);
});

test("a 401 for a key the user has since replaced keeps the replacement", () => {
  const { Auth, calls } = loadAuth();
  Auth.set(Auth.KEY_ITEM, "new-key");
  Auth.rejected("old-key");
  assert.equal(Auth.key(), "new-key");
  assert.deepEqual(calls, []);
});

test("a 401 for the stored key clears it and reports a rejection", () => {
  const { Auth, calls } = loadAuth();
  Auth.set(Auth.KEY_ITEM, "bad-key");
  Auth.rejected("bad-key");
  assert.equal(Auth.key(), "");
  assert.deepEqual(calls, [true]);
});

test("a 401 with no key anywhere prompts without calling it a rejection", () => {
  const { Auth, calls } = loadAuth();
  Auth.rejected("");
  assert.deepEqual(calls, [false]);
});

test("the admin key lives in sessionStorage and is sent only to admin routes", () => {
  const { Auth, window } = loadAuth();
  Auth.set(Auth.KEY_ITEM, "k");
  Auth.set(Auth.ADMIN_ITEM, "a");
  assert.equal(window.sessionStorage.getItem("api.adminKey"), "a");
  assert.equal(window.localStorage.getItem("api.adminKey"), null);
  assert.deepEqual(plain(Auth.headers("/api/jobs", {})), { "X-API-Key": "k" });
  assert.deepEqual(plain(Auth.headers("/api/cluster/workers/scale", { method: "POST" })), { "X-API-Key": "k", "X-Admin-Key": "a" });
  assert.deepEqual(plain(Auth.headers("/api/jobs", { method: "DELETE" })), { "X-API-Key": "k", "X-Admin-Key": "a" });
});
