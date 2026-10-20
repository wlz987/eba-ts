import { isName } from "../envelope/topic.ts";
import { InvalidTopic } from "../errors.ts";

export type Wildcard = "none" | "star" | "globstar";

export interface Pattern {
  readonly literal: readonly string[];
  readonly wildcard: Wildcard;
}

export function parsePattern(text: string): Pattern {
  if (typeof text !== "string" || !text)
    throw new InvalidTopic("empty pattern");
  const parts = text.split(".");
  if (parts.includes(""))
    throw new InvalidTopic(`illegal pattern: ${JSON.stringify(text)}`);
  for (let index = 0; index < parts.length; index += 1) {
    const part = parts[index]!;
    if (part === "*" || part === "**") {
      if (index !== parts.length - 1) {
        throw new InvalidTopic(
          `wildcard not terminal: ${JSON.stringify(text)}`,
        );
      }
      continue;
    }
    if (!isName(part))
      throw new InvalidTopic(
        `illegal pattern segment: ${JSON.stringify(part)}`,
      );
  }
  let wildcard: Wildcard = "none";
  let literal = parts;
  const last = parts[parts.length - 1]!;
  if (last === "*") {
    wildcard = "star";
    literal = parts.slice(0, -1);
  } else if (last === "**") {
    wildcard = "globstar";
    literal = parts.slice(0, -1);
  }
  return { literal, wildcard };
}

export function lookupKeys(parts: string[]): Array<[Wildcard, string]> {
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
  return keys;
}
