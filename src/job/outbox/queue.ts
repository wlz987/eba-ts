import type { Bus } from "../../mail/bus.ts";
import type { Envelope } from "../../envelope/envelope.ts";
import { DeliveryError, MailboxFull } from "../../errors.ts";
import { DELIVERY } from "../../notify/signal.ts";
import { awaited } from "./base.ts";
import { OutboxSend } from "./send.ts";

export type Step = "idle" | "checked" | "advanced";

export class OutboxQueue extends OutboxSend {
  rearm(envelope: Envelope): void {
    const group = this.groups.get(envelope.header.topic);
    if (group === undefined || group.items[0] !== envelope) return;
    this.candidates[envelope.header.topic] = null;
    this.signal.notify(DELIVERY);
  }

  bury(envelope: Envelope): void {
    const topic = envelope.header.topic;
    const group = this.groups.get(topic);
    if (group === undefined || group.items[0] !== envelope) {
      if (!this.contains(envelope)) {
        this.rememberBuried(envelope);
        this.count += 1;
      }
      return;
    }
    delete this.candidates[topic];
    group.interest.dispose();
    group.items.shift();
    this.rememberBuried(envelope);
    if (group.items.length > 0) {
      this.candidates[topic] = null;
      this.signal.notify(DELIVERY);
      return;
    }
    this.groups.delete(topic);
    this.signal.set(DELIVERY, Object.keys(this.candidates).length > 0);
  }

  flushOne(bus: Bus): [Step, Envelope | null] {
    if (Object.keys(this.candidates).length === 0) {
      this.signal.clear(DELIVERY);
      return ["idle", null];
    }
    const topic = Object.keys(this.candidates)[0]!;
    delete this.candidates[topic];
    this.signal.set(DELIVERY, Object.keys(this.candidates).length > 0);
    const group = this.groups.get(topic)!;
    const envelope = group.items[0]!;
    group.interest.dispose();
    let published: boolean | undefined;
    try {
      if (!bus.readyOrWatch(envelope, group.interest)) return ["checked", null];
      published = bus.publish(envelope);
    } catch (error) {
      if (error instanceof MailboxFull) {
        if (bus.readyOrWatch(envelope, group.interest)) {
          this.candidates[topic] = null;
          this.signal.notify(DELIVERY);
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
    this.count -= 1;
    if (group.items.length === 0) {
      group.interest.dispose();
      this.groups.delete(topic);
    } else {
      this.candidates[topic] = null;
    }
    this.signal.set(DELIVERY, Object.keys(this.candidates).length > 0);
    return ["advanced", envelope];
  }

  drain(envelopes: Envelope[]): void {
    for (const group of this.groups.values()) {
      group.interest.dispose();
      while (group.items.length > 0) envelopes.push(group.items.shift()!);
    }
    this.groups.clear();
    for (const item of this.buried) envelopes.push(item);
    this.buried = [];
    for (const key of Object.keys(this.candidates)) delete this.candidates[key];
    this.signal.clear(DELIVERY);
    this.count = 0;
  }
}
