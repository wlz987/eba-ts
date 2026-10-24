import type { Envelope } from "../envelope/envelope.ts";
import { READ, Signal } from "../notify/signal.ts";
import { Watchers } from "../notify/watch.ts";
import { Receiver } from "./receiver.ts";
export { Receiver } from "./receiver.ts";

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

const states = new WeakMap<Inbox, InboxState>();

export function stateOf(inbox: Inbox): InboxState {
  const state = states.get(inbox);
  if (state === undefined) throw new Error("invalid Inbox");
  return state;
}

function positiveCapacity(capacity: number): number {
  if (
    typeof capacity === "boolean" ||
    !Number.isInteger(capacity) ||
    capacity < 1
  ) {
    throw new RangeError("capacity must be a positive integer");
  }
  return capacity;
}

export class Inbox {
  readonly id: number;
  readonly capacity: number;
  constructor(capacity: number) {
    nextId += 1;
    this.id = nextId;
    this.capacity = positiveCapacity(capacity);
    const state: InboxState = {
      items: [],
      closed: false,
      claimed: false,
      workPaused: false,
      read: new Signal(),
      space: new Watchers(),
      routes: new Set(),
      seized: [],
    };
    states.set(this, state);
  }
  receiver(): Receiver {
    return Receiver.claim(this);
  }
  get isClosed(): boolean {
    return stateOf(this).closed;
  }
  get hasRead(): boolean {
    return stateOf(this).read.has(READ);
  }
  remaining(): number {
    return remaining(this);
  }
}
import { remaining } from "./inbox_ops.ts";
export { canEnqueue, enqueue, find, forgetRoute, heldCount, holdsKind, markRead, pauseWork, pullKind, readSignal, rememberRoute, remaining, remove, replaceTopic, replyTopic, takeSeized, watchSpace } from "./inbox_ops.ts";
