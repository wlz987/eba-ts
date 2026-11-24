import type { Bus } from "#src/core/mail/bus.js";
import { readyOrWatchOf } from "#src/core/mail/bus.js";
import type { Envelope } from "#src/core/envelope.js";
import { DeliveryError, MailboxFull } from "#src/core/errors.js";
import { DELIVERY, setCandidate } from "#src/core/notify.js";
import { awaited } from "#src/core/job/mailbox/base.js";
import {
  mailboxCandidates,
  mailboxGroups,
  mailboxSetCount,
  mailboxSignal,
  mailboxSpace,
} from "#src/core/job/mailbox/base.js";
import { MailboxSend } from "#src/core/job/mailbox/send.js";

export type Step = "idle" | "checked" | "advanced";

/**
 * Is `envelope` the head of its topic group? Identity is the envelope **id**,
 * not the object reference — see `job/delivery/parking.ts` for why. Both `rearm`
 * and `bury` match by id; comparing references would arm/bury a letter whenever
 * two distinct objects shared an id.
 */
function isHead(
  group: { items: Envelope[] },
  envelope: Envelope,
): boolean {
  const head = group.items[0];
  return head !== undefined && head.header.id === envelope.header.id;
}

export class Mailbox extends MailboxSend {
  rearm(envelope: Envelope): void {
    const group = mailboxGroups(this).get(envelope.header.topic);
    if (group === undefined || !isHead(group, envelope)) return;
    setCandidate(mailboxCandidates(this), envelope.header.topic);
    mailboxSignal(this).notify(DELIVERY);
  }

  bury(envelope: Envelope): void {
    const topic = envelope.header.topic;
    const groups = mailboxGroups(this);
    const candidates = mailboxCandidates(this);
    const signal = mailboxSignal(this);
    const group = groups.get(topic);
    if (group === undefined || !isHead(group, envelope)) return;
    delete candidates[topic];
    group.interest.dispose();
    group.items.shift();
    mailboxSetCount(this, this.count - 1);
    if (this.hasRoom()) mailboxSpace(this).notify();
    if (group.items.length > 0) {
      setCandidate(candidates, topic);
      signal.notify(DELIVERY);
      return;
    }
    groups.delete(topic);
    signal.set(DELIVERY, Object.keys(candidates).length > 0);
  }

  flushOne(bus: Bus): [Step, Envelope | null] {
    const candidates = mailboxCandidates(this);
    const signal = mailboxSignal(this);
    if (Object.keys(candidates).length === 0) {
      signal.clear(DELIVERY);
      return ["idle", null];
    }
    const topic = Object.keys(candidates)[0]!;
    delete candidates[topic];
    signal.set(DELIVERY, Object.keys(candidates).length > 0);
    // A candidate with no group is a *recoverable* state, not a crash — answer
    // idle. The two tables are written independently, so a
    // mark without a group must still return a step: throwing would abort the
    // drive's turn and, the mark already deleted, wedge the mailbox forever.
    const groups = mailboxGroups(this);
    const group = groups.get(topic);
    if (group === undefined) return ["idle", null];
    const envelope = group.items[0]!;
    group.interest.dispose();
    let published: boolean | undefined;
    try {
      if (!readyOrWatchOf(bus, envelope, group.interest)) return ["checked", null];
      published = bus.publish(envelope);
    } catch (error) {
      if (error instanceof MailboxFull) {
        if (readyOrWatchOf(bus, envelope, group.interest)) {
          setCandidate(candidates, topic);
          signal.notify(DELIVERY);
        }
        return ["checked", null];
      }
      group.interest.dispose();
      const delivery = new DeliveryError(envelope, "result publication failed");
      delivery.cause = error;
      throw delivery;
    }
    if (published === false && awaited(bus, envelope)) {
      group.interest.dispose();
      throw new DeliveryError(envelope, "result publication failed");
    }
    group.items.shift();
    mailboxSetCount(this, this.count - 1);
    if (this.hasRoom()) mailboxSpace(this).notify();
    if (group.items.length === 0) {
      group.interest.dispose();
      groups.delete(topic);
    } else {
      setCandidate(candidates, topic);
    }
    signal.set(DELIVERY, Object.keys(candidates).length > 0);
    return ["advanced", envelope];
  }

  drain(envelopes: Envelope[]): void {
    const groups = mailboxGroups(this);
    for (const group of groups.values()) {
      group.interest.dispose();
      while (group.items.length > 0) envelopes.push(group.items.shift()!);
    }
    groups.clear();
    const candidates = mailboxCandidates(this);
    for (const key of Object.keys(candidates)) delete candidates[key];
    mailboxSignal(this).clear(DELIVERY);
    mailboxSetCount(this, 0);
    mailboxSpace(this).notify();
  }
}
