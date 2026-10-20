import { rememberRoute } from "../inbox.ts";
import type { Inbox } from "../inbox.ts";
import type { Pattern } from "../../pattern/pattern.ts";
import type { Subscription } from "../bus.ts";
import { RoutesDetach } from "./detach.ts";
import { key, routeKey } from "./types.ts";

export class RoutesSubscribe extends RoutesDetach {
  subscribe(pattern: Pattern, inbox: Inbox): void {
    const wildcardIndex = this.indexes[pattern.wildcard];
    const text = key(pattern);
    let bucket = wildcardIndex.get(text);
    if (bucket === undefined) {
      bucket = new Map();
      wildcardIndex.set(text, bucket);
    }
    let entry = bucket.get(inbox.id);
    const added = entry === undefined;
    if (entry === undefined) {
      entry = { inbox, pattern, count: 0, rights: [] };
      bucket.set(inbox.id, entry);
      let group = this.byInbox.get(inbox.id);
      if (group === undefined) {
        group = new Map();
        this.byInbox.set(inbox.id, group);
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
    const entry = this.indexes[pattern.wildcard]
      .get(key(pattern))
      ?.get(inbox.id);
    if (entry !== undefined) entry.rights.push(right);
  }

}
