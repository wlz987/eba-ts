import type { Inbox } from "../inbox.ts";
import type { Pattern } from "../../pattern/pattern.ts";
import type { Subscription } from "../bus.ts";

export interface Entry {
  inbox: Inbox;
  pattern: Pattern;
  count: number;
  rights: Subscription[];
}

export type WildcardIndex = Map<string, Map<number, Entry>>;

export const key = (pattern: Pattern): string => pattern.literal.join(".");
export const routeKey = (pattern: Pattern): string =>
  `${pattern.wildcard}:${key(pattern)}`;

