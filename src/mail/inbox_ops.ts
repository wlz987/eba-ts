import type { Envelope } from "../envelope/envelope.ts";
import type { EnvelopeId } from "../envelope/ids.ts";
import type { Signal } from "../notify/signal.ts";
import { READ } from "../notify/signal.ts";
import type { Interest } from "../notify/watch.ts";
import type { Inbox, RouteOwner } from "./inbox.ts";
import { stateOf } from "./inbox.ts";

export function rememberRoute(inbox: Inbox, owner: RouteOwner): void {
  stateOf(inbox).routes.add(owner);
}

export function forgetRoute(inbox: Inbox, owner: RouteOwner): void {
  stateOf(inbox).routes.delete(owner);
}

export function enqueue(inbox: Inbox, envelope: Envelope): boolean {
  if (!canEnqueue(inbox, envelope.header.address.kind)) return false;
  stateOf(inbox).items.push(envelope);
  return true;
}

export function replaceTopic(inbox: Inbox, envelope: Envelope): boolean {
  const state = stateOf(inbox);
  const kind = envelope.header.address.kind;
  if (state.closed || (state.workPaused && (kind === "event" || kind === "request"))) {
    return false;
  }
  const kept = state.items.filter(
    (item) => item.header.topic !== envelope.header.topic,
  );
  if (kept.length >= inbox.capacity) return false;
  const removed = state.items.length - kept.length;
  state.items.length = 0;
  state.items.push(...kept);
  state.items.push(envelope);
  if (removed > 0) state.space.notify();
  return true;
}

export function canEnqueue(inbox: Inbox, kind: string): boolean {
  const state = stateOf(inbox);
  return (
    !state.closed &&
    state.items.length < inbox.capacity &&
    (!state.workPaused || (kind !== "event" && kind !== "request"))
  );
}

export function pauseWork(inbox: Inbox, paused: boolean): void {
  const state = stateOf(inbox);
  if (state.workPaused === paused) return;
  state.workPaused = paused;
  if (!paused) state.space.notify();
}

export function holdsKind(inbox: Inbox, kind: string): boolean {
  return stateOf(inbox).items.some((item) => item.header.address.kind === kind);
}

export function heldCount(inbox: Inbox): number {
  return stateOf(inbox).items.length;
}

export function pullKind(inbox: Inbox, kind: string): Envelope | null {
  const state = stateOf(inbox);
  const items = state.items;
  const full = items.length === inbox.capacity;
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index]!;
    if (item.header.address.kind !== kind) continue;
    items.splice(index, 1);
    if (items.length === 0) state.read.clear(READ);
    if (full) state.space.notify();
    return item;
  }
  return null;
}

export function remove(inbox: Inbox, envelopeId: EnvelopeId): boolean {
  const state = stateOf(inbox);
  const items = state.items;
  const full = items.length === inbox.capacity;
  for (let index = 0; index < items.length; index += 1) {
    if (items[index]!.header.id !== envelopeId) continue;
    items.splice(index, 1);
    if (items.length === 0) state.read.clear(READ);
    if (full) state.space.notify();
    return true;
  }
  return false;
}

export function find(inbox: Inbox, envelopeId: EnvelopeId): Envelope | null {
  for (const item of stateOf(inbox).items) {
    if (item.header.id === envelopeId) return item;
  }
  return null;
}

export function remaining(inbox: Inbox): number {
  const state = stateOf(inbox);
  if (state.closed) return 0;
  return inbox.capacity - state.items.length;
}

export function markRead(inbox: Inbox): void {
  stateOf(inbox).read.notify(READ);
}

export function readSignal(inbox: Inbox): Signal {
  return stateOf(inbox).read;
}

export function watchSpace(inbox: Inbox, interest: Interest): void {
  stateOf(inbox).space.watch(interest);
}

export function replyTopic(inbox: Inbox): string {
  return `sys.reply.i${inbox.id}`;
}

export function takeSeized(inbox: Inbox): Envelope[] {
  const state = stateOf(inbox);
  const found = [...state.seized];
  state.seized.length = 0;
  return found;
}
