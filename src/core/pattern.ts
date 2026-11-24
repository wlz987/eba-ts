export type Wildcard = "none" | "star" | "globstar";

export interface Pattern {
  readonly literal: readonly string[];
  readonly wildcard: Wildcard;
}

/** An owned copy of `pattern`, safe to store in the route table. */
export function copyPattern(pattern: Pattern): Pattern {
  return {
    literal: [...pattern.literal],
    wildcard: pattern.wildcard,
  };
}

export function parsePattern(text: string): Pattern {
  const parts = text.split(".");
  let wildcard: Wildcard = "none";
  let literal = parts;
  const last = parts[parts.length - 1]!;
  if (last === "*") wildcard = "star";
  else if (last === "**") wildcard = "globstar";
  if (wildcard !== "none") literal = parts.slice(0, -1);
  return { literal, wildcard };
}

/** The index keys a concrete topic must be looked up under. */
const keyCache = new WeakMap<
  readonly string[],
  readonly (readonly [Wildcard, string])[]
>();

export function lookupKeys(parts: string[]): Array<[Wildcard, string]> {
  const cached = keyCache.get(parts);
  if (cached !== undefined) return cached as Array<[Wildcard, string]>;
  const keys: Array<[Wildcard, string]> = [["none", parts.join(".")]];
  if (parts.length > 0) {
    keys.push(["star", parts.slice(0, -1).join(".")]);
  }
  keys.push(["globstar", ""]);
  let prefix = "";
  for (const part of parts) {
    prefix = prefix === "" ? part : `${prefix}.${part}`;
    keys.push(["globstar", prefix]);
  }
  keyCache.set(parts, keys);
  return keys;
}
