import { Changes } from "#src/core/notify.js";
import type { Inbox } from "#src/core/mail/inbox.js";
import type { Pattern, Wildcard } from "#src/core/pattern.js";
import type { Subscription } from "#src/core/mail/bus.js";

/**
 * The route table: three wildcard indexes plus the per-inbox reverse index.
 * `Routes` is reachable through `Bus`, so callers use these narrow accessors.
 */

export function routeIndexes(routes: RoutesBase): Record<Wildcard, WildcardIndex> {
  return routes.$indexes;
}

export function routeByInbox(routes: RoutesBase): Map<number, Map<string, Entry>> {
  return routes.$byInbox;
}

export class RoutesBase {
  $indexes: Record<Wildcard, WildcardIndex> = {
    none: new Map(),
    star: new Map(),
    globstar: new Map(),
  };
  $byInbox = new Map<number, Map<string, Entry>>();
  readonly changes = new Changes();

}

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
