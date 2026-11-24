import { forgetRoute } from "#src/core/mail/inbox.js";
import type { Inbox } from "#src/core/mail/inbox.js";
import type { Pattern } from "#src/core/pattern.js";
import type { Subscription } from "#src/core/mail/bus.js";
import { disarmSubscription } from "#src/core/mail/bus/types.js";
import { RoutesBase, routeByInbox, routeIndexes, key, routeKey } from "#src/core/mail/routes/base.js";

export class RoutesDetach extends RoutesBase {
  detach(
    pattern: Pattern,
    inbox: Inbox,
    right?: Subscription,
  ): [boolean, Pattern | null] {
    const text = key(pattern);
    const bucket = routeIndexes(this)[pattern.wildcard].get(text);
    if (bucket === undefined || !bucket.has(inbox.id)) {
      if (right !== undefined) disarmSubscription(right);
      return [false, null];
    }
    const entry = bucket.get(inbox.id)!;
    if (right !== undefined) {
      entry.rights = entry.rights.filter((item) => item !== right);
    }
    if (entry.count > 1) {
      entry.count -= 1;
      if (right !== undefined) disarmSubscription(right);
      return [true, null];
    }
    bucket.delete(inbox.id);
    if (bucket.size === 0) routeIndexes(this)[pattern.wildcard].delete(text);
    const group = routeByInbox(this).get(inbox.id);
    if (group !== undefined) {
      group.delete(routeKey(pattern));
      if (group.size === 0) {
        routeByInbox(this).delete(inbox.id);
        forgetRoute(inbox, this);
      }
    }
    if (right !== undefined) disarmSubscription(right);
    return [true, entry.pattern];
  }

  unsubscribeAll(inbox: Inbox): void {
    const group = routeByInbox(this).get(inbox.id);
    if (group === undefined) return;
    routeByInbox(this).delete(inbox.id);
    const patterns: Pattern[] = [];
    for (const [, entry] of group) {
      const text = key(entry.pattern);
      const bucket = routeIndexes(this)[entry.pattern.wildcard].get(text);
      if (bucket !== undefined) {
        bucket.delete(inbox.id);
        if (bucket.size === 0)
          routeIndexes(this)[entry.pattern.wildcard].delete(text);
      }
      for (const item of entry.rights) disarmSubscription(item);
      entry.rights.length = 0;
      patterns.push(entry.pattern);
    }
    for (const pattern of patterns) this.changes.notify(pattern);
  }

}
