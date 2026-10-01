// Advisory check for prerelease-tagged production dependencies.
//
// `npm audit` matches advisory ranges with node-semver, which never counts a
// prerelease-tagged version as inside a range such as "<2.0.0": multer
// 1.4.5-lts.2 passed it with ten high advisories open against it. GitHub's
// advisory database does match such versions, so every prerelease-tagged
// package outside the dev-only closure is looked up there.
//
// Usage: node tools/audit-prerelease.js [path/to/package-lock.json]
// Exit 0: no high/critical match. 1: at least one. 2: a lookup failed (fails closed).
// GITHUB_TOKEN, when set, authenticates the lookups.
//
// Lives in tools/, not a test/ directory, so `node --test` never executes it.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PRERELEASE = /^\d+\.\d+\.\d+-/;
const BLOCKING = new Set(["high", "critical"]);
const PAGE = 100;

// Unique {name, version} pairs installed at a prerelease-tagged version outside
// the dev-only closure (`npm ci --omit=dev` still installs devOptional), sorted.
export function prereleasePackages(lock) {
  const found = new Map();
  for (const [key, entry] of Object.entries(lock.packages ?? {})) {
    const at = key.lastIndexOf("node_modules/");
    if (at === -1 || entry.dev || entry.link || !PRERELEASE.test(entry.version ?? "")) continue;
    const name = entry.name ?? key.slice(at + "node_modules/".length);
    found.set(`${name}@${entry.version}`, { name, version: entry.version });
  }
  return [...found.values()].sort((a, b) =>
    a.name < b.name ? -1 : a.name > b.name ? 1 : a.version < b.version ? -1 : a.version > b.version ? 1 : 0);
}

// Advisories that fail the gate: same threshold as `npm audit --audit-level=high`.
export function blockingAdvisories(advisories) {
  return advisories.filter((a) => !a.withdrawn_at && BLOCKING.has(a.severity));
}

async function advisoriesFor({ name, version }, token) {
  const url = new URL("https://api.github.com/advisories");
  url.searchParams.set("ecosystem", "npm");
  url.searchParams.set("affects", `${name}@${version}`);
  url.searchParams.set("per_page", String(PAGE));
  const headers = { Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28" };
  if (token) headers.Authorization = `Bearer ${token}`;
  const r = await fetch(url, { headers });
  if (!r.ok) throw new Error(`advisory lookup for ${name}@${version}: HTTP ${r.status}`);
  const list = await r.json();
  if (list.length >= PAGE) throw new Error(`advisory lookup for ${name}@${version}: ${PAGE}+ results, not paged`);
  return list;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const file = process.argv[2] ?? path.join(REPO_ROOT, "package-lock.json");
  const pkgs = prereleasePackages(JSON.parse(fs.readFileSync(file, "utf8")));
  let blocking = 0;
  try {
    for (const p of pkgs) {
      const all = await advisoriesFor(p, process.env.GITHUB_TOKEN);
      const block = new Set(blockingAdvisories(all));
      for (const a of all) {
        console.log(`${block.has(a) ? "BLOCK" : "note "} ${p.name}@${p.version} ${a.ghsa_id} ${a.severity} ${a.summary}`);
      }
      blocking += block.size;
    }
  } catch (err) {
    console.error(`audit-prerelease: ${err.message}`);
    process.exit(2);
  }
  console.log(`audit-prerelease: ${pkgs.length} prerelease-tagged production package(s), ${blocking} blocking advisory match(es)`);
  process.exit(blocking ? 1 : 0);
}
