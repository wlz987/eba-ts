import type { Bus } from "#src/core/mail/bus.js";
import type { Envelope } from "#src/core/envelope.js";
import { DeliveryError, MailboxFull, MailboxBusy } from "#src/core/errors.js";
import { DELIVERY, Interest, setCandidate } from "#src/core/notify.js";
import { readyOrWatchOf } from "#src/core/mail/bus.js";
import {
  MailboxBase,
  awaited,
  mailboxCandidates,
  mailboxGroups,
  mailboxSetCount,
  mailboxSignal,
} from "#src/core/job/mailbox/base.js";

export class MailboxSend extends MailboxBase {
  send(bus: Bus, envelope: Envelope): void {
    const topic = envelope.header.topic;
    const groups = mailboxGroups(this);
    let group = groups.get(topic);
    if (group === undefined) {
      let direct = true;
      let published: boolean | undefined;
      try {
        published = bus.publish(envelope);
      } catch (error) {
        if (error instanceof MailboxFull) {
          direct = false;
        } else {
          const delivery = new DeliveryError(
            envelope,
            "result publication failed",
          );
          delivery.cause = error;
          throw delivery;
        }
      }
      if (direct) {
        if (published === false && awaited(bus, envelope)) {
          throw new DeliveryError(envelope, "result publication failed");
        }
        return;
      }
    }
    if (this.count >= this.limit)
      throw new MailboxBusy(envelope);
    const first = group === undefined;
    if (group === undefined) {
      group = {
        interest: new Interest(
          mailboxSignal(this),
          mailboxCandidates(this),
          topic,
        ),
        items: [],
      };
      groups.set(topic, group);
    }
    group.items.push(envelope);
    mailboxSetCount(this, this.count + 1);
    if (!first) return;
    try {
      if (readyOrWatchOf(bus, envelope, group.interest)) {
        setCandidate(mailboxCandidates(this), topic);
        mailboxSignal(this).notify(DELIVERY);
      }
    } catch (error) {
      group.interest.dispose();
      mailboxSetCount(this, this.count - 1);
      groups.delete(topic);
      const delivery = new DeliveryError(
        envelope,
        "result delivery registration failed",
      );
      delivery.cause = error;
      throw delivery;
    }
  }
}
