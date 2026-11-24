import type { Bus } from "#src/core/mail/bus.js";
import type { Envelope } from "#src/core/envelope.js";
import { correlationIdOf } from "#src/core/envelope.js";

import type { Signal } from "#src/core/notify.js";
import { Interest, Watchers } from "#src/core/notify.js";

export function awaited(bus: Bus, envelope: Envelope): boolean {
  return bus.waiting(correlationIdOf(envelope.header.address));
}

/**
 * One topic's parked letters plus the interest that wakes the mailbox for them.
 */
export interface TopicGroup {
  readonly interest: Interest;
  readonly items: Envelope[];
}

export function mailboxSignal(box: MailboxBase): Signal {
  return box.$signal;
}

export function mailboxCandidates(box: MailboxBase): Record<string, null> {
  return box.$candidates;
}

export function mailboxGroups(box: MailboxBase): Map<string, TopicGroup> {
  return box.$groups;
}

/** Advance the parked-letter counter (the one place the bookkeeping moves). */
export function mailboxSetCount(box: MailboxBase, value: number): void {
  box.$count = value;
}

export function mailboxSpace(box: MailboxBase): Watchers {
  return box.$space;
}

export class MailboxBase {
  limit: number;
  $signal: Signal;
  $candidates: Record<string, null> = {};
  $groups = new Map<string, TopicGroup>();
  $count = 0;
  /** The space watcher, pinned as an ECMAScript `#` field behind a getter-only */
  $space = new Watchers();

  constructor(limit: number, signal: Signal) {
    this.limit = limit;
    this.$signal = signal;
  }

  get count(): number {
    return this.$count;
  }

  get space(): Watchers {
    return this.$space;
  }

  get size(): number {
    return this.$count;
  }

  hasRoom(): boolean {
    return this.$count < this.limit;
  }

  contains(envelope: Envelope): boolean {
    const id = envelope.header.id;
    for (const group of this.$groups.values()) {
      if (group.items.some((item) => item.header.id === id)) return true;
    }
    return false;
  }
}
