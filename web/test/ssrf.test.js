import { test } from "node:test";
import assert from "node:assert/strict";

import { assertPublicUrl, isBlockedAddress } from "../lib/ssrf.js";

/** Build a fake DNS resolver that records calls. */
function fakeLookup(addresses) {
  const calls = [];
  const fn = async (hostname, opts) => {
    calls.push({ hostname, opts });
    return addresses;
  };
  fn.calls = calls;
  return fn;
}

const v4 = (address) => ({ address, family: 4 });
const v6 = (address) => ({ address, family: 6 });

test("isBlockedAddress covers every private, loopback and link-local range", () => {
  const blocked = [
    ["127.0.0.1", 4], ["127.255.255.254", 4],
    ["10.0.0.1", 4], ["10.255.255.255", 4],
    ["172.16.0.1", 4], ["172.31.255.254", 4],
    ["192.168.1.1", 4],
    ["169.254.169.254", 4],
    ["100.64.0.1", 4],
    ["0.0.0.0", 4],
    ["::1", 6],
    ["fc00::1", 6], ["fd12:3456::1", 6],
    ["fe80::1", 6],
  ];
  for (const [addr, family] of blocked) {
    assert.equal(isBlockedAddress(addr, family), true, `${addr} should be blocked`);
  }
});

test("isBlockedAddress permits ordinary public addresses", () => {
  for (const [addr, family] of [["93.184.216.34", 4], ["8.8.8.8", 4], ["172.32.0.1", 4], ["2606:2800:220:1::1", 6]]) {
    assert.equal(isBlockedAddress(addr, family), false, `${addr} should be allowed`);
  }
});

test("IPv4-mapped IPv6 addresses are unwrapped before the check", () => {
  // ::ffff:127.0.0.1 reaches loopback but will not match an IPv4 subnet unless
  // it is normalised first — this is the bypass the normalisation closes.
  assert.equal(isBlockedAddress("::ffff:127.0.0.1", 6), true);
  assert.equal(isBlockedAddress("::ffff:169.254.169.254", 6), true);
  assert.equal(isBlockedAddress("::ffff:10.0.0.1", 6), true);
  assert.equal(isBlockedAddress("::ffff:93.184.216.34", 6), false);
});

test("assertPublicUrl rejects each blocked target by hostname resolution", async () => {
  const targets = [
    "127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1",
    "169.254.169.254", "100.64.0.1", "0.0.0.0",
  ];
  for (const addr of targets) {
    const lookup = fakeLookup([v4(addr)]);
    await assert.rejects(
      () => assertPublicUrl("http://victim.example/x", { lookup }),
      /URL not allowed/,
      `${addr} should be refused`,
    );
  }
  for (const addr of ["::1", "fc00::1", "fe80::1", "::ffff:127.0.0.1"]) {
    const lookup = fakeLookup([v6(addr)]);
    await assert.rejects(() => assertPublicUrl("http://victim.example/x", { lookup }), /URL not allowed/, addr);
  }
});

test("assertPublicUrl permits a public host and returns its addresses", async () => {
  const lookup = fakeLookup([v4("93.184.216.34")]);
  const out = await assertPublicUrl("https://example.com/video.mp4", { lookup });
  assert.equal(out.hostname, "example.com");
  assert.equal(out.protocol, "https:");
  assert.deepEqual(out.addresses, [v4("93.184.216.34")]);
  assert.equal(lookup.calls.length, 1);
  // `all` is what makes a multi-record answer checkable at all.
  assert.equal(lookup.calls[0].opts.all, true);
});

test("one private address among several public ones still refuses", async () => {
  const lookup = fakeLookup([v4("93.184.216.34"), v4("8.8.8.8"), v4("169.254.169.254")]);
  await assert.rejects(() => assertPublicUrl("http://dual.example/x", { lookup }), /URL not allowed/);
});

test("an empty resolution is refused rather than treated as public", async () => {
  const lookup = fakeLookup([]);
  await assert.rejects(() => assertPublicUrl("http://void.example/x", { lookup }), /URL not allowed/);
});

test("non-http schemes are refused before any DNS work happens", async () => {
  for (const url of ["file:///etc/passwd", "gopher://x/1", "ftp://x/y", "data:text/plain,hi"]) {
    const lookup = fakeLookup([v4("93.184.216.34")]);
    await assert.rejects(() => assertPublicUrl(url, { lookup }), /URL not allowed/, url);
    assert.equal(lookup.calls.length, 0, `${url} must not be resolved`);
  }
});

test("an unparseable URL is refused", async () => {
  const lookup = fakeLookup([v4("93.184.216.34")]);
  await assert.rejects(() => assertPublicUrl("not a url", { lookup }), /URL not allowed/);
  assert.equal(lookup.calls.length, 0);
});
