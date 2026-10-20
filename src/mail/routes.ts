import type { Inbox } from "./inbox.ts";
import { lookupKeys } from "../pattern/pattern.ts";
import type { Entry } from "./routes/types.ts";
import { RoutesSubscribe } from "./routes/subscribe.ts";

export class Routes extends RoutesSubscribe {
  private *buckets(parts: string[]): Generator<Map<number, Entry>> {
    for (const [kind, text] of lookupKeys(parts)) {
      const bucket = this.indexes[kind].get(text);
      if (bucket !== undefined && bucket.size > 0) yield bucket;
    }
  }

  snapshotMatch(parts: string[]): Inbox[] {
    const targets = new Map<number, Inbox>();
    for (const bucket of this.buckets(parts)) {
      for (const [inboxId, entry] of bucket) {
        if (!targets.has(inboxId)) targets.set(inboxId, entry.inbox);
      }
    }
    return [...targets.values()];
  }

  holdCount(parts: string[], inbox: Inbox): number {
    let total = 0;
    for (const bucket of this.buckets(parts)) {
      const entry = bucket.get(inbox.id);
      if (entry !== undefined && entry.inbox === inbox) total += entry.count;
    }
    return total;
  }
}

