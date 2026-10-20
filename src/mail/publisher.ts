import type { Envelope } from "../envelope/envelope.ts";
import { makeEnvelope } from "../envelope/envelope.ts";
import type { IdGen } from "../envelope/idgen.ts";
import { actorId } from "../envelope/ids.ts";
import type { ActorId } from "../envelope/ids.ts";
import { DeliveryError } from "../errors.ts";
import type { Bus } from "./bus.ts";

export class Publisher {
  readonly actorId: ActorId;
  constructor(actor: ActorId | string) {
    this.actorId = actorId(actor);
  }
  emit(
    bus: Bus,
    options: { topic: string; payload: unknown; idGen: IdGen },
  ): Envelope {
    const envelope = makeEnvelope({
      topic: options.topic,
      payload: options.payload,
      from: this.actorId,
      idGen: options.idGen,
    });
    try {
      bus.publish(envelope);
    } catch (error) {
      const delivery = new DeliveryError(envelope, "publication failed");
      delivery.cause = error;
      throw delivery;
    }
    return envelope;
  }
}
