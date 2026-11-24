import type { Envelope, EnvelopeId } from "#src/core/envelope.js";
import type { Signal, Interest } from "#src/core/notify.js";
import { READ } from "#src/core/notify.js";
import type { Inbox, RouteOwner } from "#src/core/mail/inbox.js";
import { schemasOf, stateOf } from "#src/core/mail/inbox.js";

/**
 * Migrate `envelope` to the inbox registry's current schema version. Fail
 * closed: an unknown topic is left alone, anything else is the registry's
 * verdict.
 */
export function migrateRead(inbox: Inbox, envelope: Envelope): Envelope {
  const registry = schemasOf(inbox);
  return registry === null ? envelope : registry.migrateEnvelope(envelope);
}

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

export function canEnqueue(inbox: Inbox, kind: string): boolean {
  const state = stateOf(inbox);
  return (
    !state.closed &&
    state.items.length < inbox.capacity &&
    (!state.workPaused || (kind !== "event" && kind !== "request"))
  );
}

/**
 * Pause or resume work intake for this inbox.
 *
 * Store the verdict as handed over, not by truthiness — a string, number or
 * `null` slipping past the erased `boolean` type would corrupt the flag. The
 * same gate is used on `Signal.set` / `Schedule` / `Agenda.update`.
 */
export function pauseWork(inbox: Inbox, paused: boolean): void {
  const verdict = paused;
  const state = stateOf(inbox);
  if (state.workPaused === verdict) return;
  state.workPaused = verdict;
  if (!verdict) state.space.notify();
}

export function holdsKind(inbox: Inbox, kind: string): boolean {
  return stateOf(inbox).items.some((item) => item.header.address.kind === kind);
}

export function heldCount(inbox: Inbox): number {
  return stateOf(inbox).items.length;
}

/** 首个匹配项出队：清空时撤 READ、腾位时唤醒 space（两调用方共享的副作用）。 */
function takeWhere(
  inbox: Inbox,
  matches: (item: Envelope) => boolean,
): Envelope | null {
  const state = stateOf(inbox);
  const items = state.items;
  const full = items.length === inbox.capacity;
  const index = items.findIndex(matches);
  if (index < 0) return null;
  const [item] = items.splice(index, 1);
  if (items.length === 0) state.read.clear(READ);
  if (full) state.space.notify();
  return item ?? null;
}

export function pullKind(inbox: Inbox, kind: string): Envelope | null {
  return takeWhere(inbox, (item) => item.header.address.kind === kind);
}

export function remove(inbox: Inbox, envelopeId: EnvelopeId): boolean {
  return takeWhere(inbox, (item) => item.header.id === envelopeId) !== null;
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
