import type { Bus } from "../../mail/bus.ts";
import type { Envelope } from "../../envelope/envelope.ts";
import { DeliveryError, MailboxFull, OutboxFull } from "../../errors.ts";
import { DELIVERY } from "../../notify/signal.ts";
import { Interest } from "../../notify/watch.ts";
import { Group, OutboxBase, awaited } from "./base.ts";

export class OutboxSend extends OutboxBase {
  send(bus: Bus, envelope: Envelope): void {
    const topic = envelope.header.topic;
    let group = this.groups.get(topic);
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
    if (this.queued() >= this.limit)
      throw new OutboxFull(envelope);
    const first = group === undefined;
    if (group === undefined) {
      group = new Group(new Interest(this.signal, this.candidates, topic));
      this.groups.set(topic, group);
    }
    group.items.push(envelope);
    this.count += 1;
    if (!first) return;
    try {
      if (bus.readyOrWatch(envelope, group.interest)) {
        this.candidates[topic] = null;
        this.signal.notify(DELIVERY);
      }
    } catch (error) {
      group.interest.dispose();
      this.count -= 1;
      this.groups.delete(topic);
      const delivery = new DeliveryError(
        envelope,
        "result delivery registration failed",
      );
      delivery.cause = error;
      throw delivery;
    }
  }

}
