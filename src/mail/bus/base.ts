import { splitTopic } from "../../envelope/topic.ts";
import { parsePattern } from "../../pattern/pattern.ts";
import type { Inbox } from "../inbox.ts";
import { Routes } from "../routes.ts";
import { Offer, Subscription } from "./types.ts";

export class BusBase {
  protected readonly offers: Offer[] = [];
  protected readonly waits = new Set<string>();

  protected routes = new Routes();
  protected grantFrames: Array<Array<[number, Subscription]>> = [];

  openGrants(): Array<[number, Subscription]> {
    const frame: Array<[number, Subscription]> = [];
    this.grantFrames.push(frame);
    return frame;
  }

  closeGrants(): void {
    this.grantFrames.pop();
  }

  noteGrant(inbox: Inbox, granted: Subscription): void {
    if (this.grantFrames.length > 0) {
      this.grantFrames[this.grantFrames.length - 1]!.push([inbox.id, granted]);
    }
  }

  subscribe(pattern: string, inbox: Inbox): Subscription {
    if (inbox.isClosed) throw new Error("inbox closed");
    const parsed = parsePattern(pattern);
    this.routes.subscribe(parsed, inbox);
    const granted = new Subscription(() => {
      const [found, pending] = this.routes.detach(parsed, inbox, granted);
      if (pending !== null) this.routes.changes.notify(pending);
      return found;
    });
    this.routes.bind(parsed, inbox, granted);
    this.noteGrant(inbox, granted);
    return granted;
  }

  subscriptionCount(topic: string, inbox: Inbox): number {
    let parts: string[];
    try {
      parts = splitTopic(topic);
    } catch {
      return 0;
    }
    return this.routes.holdCount(parts, inbox);
  }

}
