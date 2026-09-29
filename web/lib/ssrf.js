import dns from "node:dns";
import net from "node:net";

/**
 * Address space a server-side fetcher must never reach: loopback, RFC1918,
 * link-local (cloud metadata lives at 169.254.169.254), carrier-grade NAT,
 * the unspecified block, and their IPv6 equivalents.
 */
export const BLOCKED_CIDRS = Object.freeze([
  ["127.0.0.0", 8, "ipv4"],
  ["10.0.0.0", 8, "ipv4"],
  ["172.16.0.0", 12, "ipv4"],
  ["192.168.0.0", 16, "ipv4"],
  ["169.254.0.0", 16, "ipv4"],
  ["100.64.0.0", 10, "ipv4"],
  ["0.0.0.0", 8, "ipv4"],
  ["::1", 128, "ipv6"],
  ["fc00::", 7, "ipv6"],
  ["fe80::", 10, "ipv6"],
]);

const blockList = new net.BlockList();
for (const [addr, prefix, type] of BLOCKED_CIDRS) {
  blockList.addSubnet(addr, prefix, type);
}

/**
 * Strip the ::ffff: wrapper from an IPv4-mapped IPv6 address.
 * Without this, `::ffff:127.0.0.1` reaches loopback while matching no IPv4
 * subnet — a complete bypass of the list above.
 */
function normalize(address) {
  const m = /^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/i.exec(address);
  if (m) return { address: m[1], type: "ipv4" };
  if (net.isIPv4(address)) return { address, type: "ipv4" };
  if (net.isIPv6(address)) return { address, type: "ipv6" };
  return null;
}

/** True when `address` falls inside any blocked range. */
export function isBlockedAddress(address) {
  const n = normalize(String(address));
  if (!n) return true; // unparseable is never safe to dial
  return blockList.check(n.address, n.type);
}

/**
 * Resolve `rawUrl` and refuse it unless every resolved address is public.
 *
 * Returns the resolved addresses so the caller can connect to a specific IP
 * rather than re-resolving — re-resolution reopens the DNS-rebinding window
 * between this check and the connection.
 *
 * `lookup` is injectable so tests need no DNS.
 */
export async function assertPublicUrl(rawUrl, { lookup = dns.promises.lookup } = {}) {
  let parsed;
  try {
    parsed = new URL(rawUrl);
  } catch {
    throw new Error("URL not allowed");
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error("URL not allowed");
  }

  // URL keeps IPv6 literals in brackets; the resolver wants them bare.
  const hostname = parsed.hostname.replace(/^\[|\]$/g, "");

  let addresses;
  try {
    addresses = await lookup(hostname, { all: true, verbatim: true });
  } catch {
    throw new Error("URL not allowed");
  }
  if (!Array.isArray(addresses) || addresses.length === 0) {
    throw new Error("URL not allowed");
  }
  // Every answer must be public: a single private record is enough to pivot.
  for (const entry of addresses) {
    if (isBlockedAddress(entry.address)) throw new Error("URL not allowed");
  }

  return { hostname, port: parsed.port, protocol: parsed.protocol, addresses };
}
