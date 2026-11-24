import type { Envelope } from "#src/core/envelope.js";
import { Signal, Watchers } from "#src/core/notify.js";
import { Receiver } from "#src/core/mail/receiver.js";
import type { SchemaRegistry } from "#src/core/schema.js";
export { Receiver } from "#src/core/mail/receiver.js";

export const TOKEN = Symbol("inbox-token");
let nextId = 0;
let closes = 0;

export function closeCount(): number {
  return closes;
}

export function noteClose(): void {
  closes += 1;
}

export interface RouteOwner {
  unsubscribeAll(inbox: Inbox): void;
}

export interface InboxState {
  items: Envelope[];
  closed: boolean;
  claimed: boolean;
  workPaused: boolean;
  read: Signal;
  space: Watchers;
  routes: Set<RouteOwner>;
  seized: Envelope[];
}

export function stateOf(inbox: Inbox): InboxState {
  return inbox.$state;
}

/** The inbox's schema registry (internal; reads migrate through it). */
export function schemasOf(inbox: Inbox): SchemaRegistry | null {
  return inbox.$schemas;
}

export class Inbox {
  /** Identity and bound, pinned as ECMAScript `#` fields. */
  readonly id: number;
  readonly capacity: number;
  readonly $state: InboxState;
  /** Optional schema registry; when set, reads migrate older payloads. */
  readonly $schemas: SchemaRegistry | null;

  constructor(capacity: number, schemas?: SchemaRegistry) {
    nextId += 1;
    this.id = nextId;
    this.capacity = capacity;
    this.$schemas = schemas ?? null;
    this.$state = {
      items: [],
      closed: false,
      claimed: false,
      workPaused: false,
      read: new Signal(),
      space: new Watchers(),
      routes: new Set(),
      seized: [],
    };
  }


  receiver(): Receiver {
    return Receiver.claim(this);
  }
  get isClosed(): boolean {
    return this.$state.closed;
  }
  remaining(): number {
    return remaining(this);
  }
}
import { remaining } from "#src/core/mail/inbox_ops.js";
export { canEnqueue, enqueue, find, forgetRoute, heldCount, holdsKind, markRead, pauseWork, pullKind, readSignal, rememberRoute, remaining, remove, replyTopic, takeSeized, watchSpace } from "#src/core/mail/inbox_ops.js";
