import { rememberRoute } from "#src/core/mail/inbox.js";
import type { Inbox } from "#src/core/mail/inbox.js";
import { copyPattern } from "#src/core/pattern.js";
import type { Pattern } from "#src/core/pattern.js";
import type { Subscription } from "#src/core/mail/bus.js";
import { RoutesDetach } from "#src/core/mail/routes/detach.js";
import { routeByInbox, routeIndexes, key, routeKey } from "#src/core/mail/routes/base.js";

export class RoutesSubscribe extends RoutesDetach {
  subscribe(pattern: Pattern, inbox: Inbox): void {
    const wildcardIndex = routeIndexes(this)[pattern.wildcard];
    const text = key(pattern);
    let bucket = wildcardIndex.get(text);
    if (bucket === undefined) {
      bucket = new Map();
      wildcardIndex.set(text, bucket);
    }
    let entry = bucket.get(inbox.id);
    const added = entry === undefined;
    if (entry === undefined) {
      // The entry gets its **own** `Pattern`, never the caller's: the forward
      // index is keyed here but `detach`/`unsubscribeAll` recompute the key from
      // `entry.pattern` later, so aliasing the caller's object lets one write
      // strand an entry the route table can no longer remove — permanently
      // routing to a closed inbox. Cold path: once per *new* route.
      entry = { inbox, pattern: copyPattern(pattern), count: 0, rights: [] };
      bucket.set(inbox.id, entry);
      let group = routeByInbox(this).get(inbox.id);
      if (group === undefined) {
        group = new Map();
        routeByInbox(this).set(inbox.id, group);
        rememberRoute(inbox, this);
      }
      group.set(routeKey(pattern), entry);
    }
    entry.count += 1;
    if (!added) return;
    try {
      this.changes.notify(pattern);
    } catch (error) {
      this.detach(pattern, inbox);
      throw error;
    }
  }

  bind(pattern: Pattern, inbox: Inbox, right: Subscription): void {
    const entry = routeIndexes(this)[pattern.wildcard]
      .get(key(pattern))
      ?.get(inbox.id);
    if (entry !== undefined) entry.rights.push(right);
  }

}
