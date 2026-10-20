import { forgetRoute } from "../inbox.ts";
import type { Inbox } from "../inbox.ts";
import type { Pattern } from "../../pattern/pattern.ts";
import type { Subscription } from "../bus.ts";
import { RoutesBase } from "./base.ts";
import { key, routeKey } from "./types.ts";

export class RoutesDetach extends RoutesBase {
  detach(
    pattern: Pattern,
    inbox: Inbox,
    right?: Subscription,
  ): [boolean, Pattern | null] {
    const text = key(pattern);
    const bucket = this.indexes[pattern.wildcard].get(text);
    if (bucket === undefined || !bucket.has(inbox.id)) {
      right?.disarm();
      return [false, null];
    }
    const entry = bucket.get(inbox.id)!;
    if (right !== undefined) {
      entry.rights = entry.rights.filter((item) => item !== right);
    }
    if (entry.count > 1) {
      entry.count -= 1;
      right?.disarm();
      return [true, null];
    }
    bucket.delete(inbox.id);
    if (bucket.size === 0) this.indexes[pattern.wildcard].delete(text);
    const group = this.byInbox.get(inbox.id);
    if (group !== undefined) {
      group.delete(routeKey(pattern));
      if (group.size === 0) {
        this.byInbox.delete(inbox.id);
        forgetRoute(inbox, this);
      }
    }
    right?.disarm();
    return [true, entry.pattern];
  }

  unsubscribeAll(inbox: Inbox): void {
    const group = this.byInbox.get(inbox.id);
    if (group === undefined) return;
    this.byInbox.delete(inbox.id);
    const patterns: Pattern[] = [];
    for (const [, entry] of group) {
      const text = key(entry.pattern);
      const bucket = this.indexes[entry.pattern.wildcard].get(text);
      if (bucket !== undefined) {
        bucket.delete(inbox.id);
        if (bucket.size === 0)
          this.indexes[entry.pattern.wildcard].delete(text);
      }
      for (const item of entry.rights) item.disarm();
      entry.rights.length = 0;
      patterns.push(entry.pattern);
    }
    for (const pattern of patterns) this.changes.notify(pattern);
  }

}
