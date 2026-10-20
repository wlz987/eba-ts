import { Changes } from "../../notify/watch.ts";
import type { Wildcard } from "../../pattern/pattern.ts";
import type { Entry, WildcardIndex } from "./types.ts";

export class RoutesBase {
  protected indexes: Record<Wildcard, WildcardIndex> = {
    none: new Map(),
    star: new Map(),
    globstar: new Map(),
  };
  protected byInbox = new Map<number, Map<string, Entry>>();
  readonly changes = new Changes();

}
