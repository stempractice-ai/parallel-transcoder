// Offline checks of which lockfile entries the prerelease advisory gate looks
// up, and which advisories fail it. The network lookup is exercised in CI.
import { test } from "node:test";
import assert from "node:assert/strict";

import { prereleasePackages, blockingAdvisories } from "./audit-prerelease.js";

test("only prerelease-tagged packages outside the dev-only closure are looked up", () => {
  const lock = {
    lockfileVersion: 3,
    packages: {
      "": { name: "parallel-transcoder", version: "1.0.0-beta.1" },
      "node_modules/multer": { version: "1.4.5-lts.2" },
      "node_modules/express": { version: "4.21.2" },
      "node_modules/app-builder-bin": { version: "5.0.0-alpha.10", dev: true },
      "node_modules/opt-tool": { version: "2.0.0-rc.1", devOptional: true },
      "node_modules/a/node_modules/@scope/x": { version: "1.0.0-beta.1" },
      "node_modules/b/node_modules/@scope/x": { version: "1.0.0-beta.1" },
      "node_modules/alias": { name: "real-pkg", version: "3.0.0-next.0" },
      "node_modules/linked": { link: true, resolved: "packages/linked" },
    },
  };
  assert.deepEqual(prereleasePackages(lock), [
    { name: "@scope/x", version: "1.0.0-beta.1" },
    { name: "multer", version: "1.4.5-lts.2" },
    { name: "opt-tool", version: "2.0.0-rc.1" },
    { name: "real-pkg", version: "3.0.0-next.0" },
  ]);
});

test("high and critical advisories block; withdrawn and lower ones do not", () => {
  const advisories = [
    { ghsa_id: "GHSA-1", severity: "high", withdrawn_at: null },
    { ghsa_id: "GHSA-2", severity: "critical", withdrawn_at: null },
    { ghsa_id: "GHSA-3", severity: "medium", withdrawn_at: null },
    { ghsa_id: "GHSA-4", severity: "low", withdrawn_at: null },
    { ghsa_id: "GHSA-5", severity: "high", withdrawn_at: "2026-01-01T00:00:00Z" },
  ];
  assert.deepEqual(blockingAdvisories(advisories).map((a) => a.ghsa_id), ["GHSA-1", "GHSA-2"]);
});
