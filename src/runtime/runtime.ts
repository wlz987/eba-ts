import type { Bus } from "../mail/bus.ts";
import type { Clock } from "../time/clock.ts";
import type { Envelope } from "../envelope/envelope.ts";
import { makeEnvelope } from "../envelope/envelope.ts";
import type { IdGen } from "../envelope/idgen.ts";
import { correlationIdOf } from "../envelope/header.ts";
import { errPayload } from "../envelope/result.ts";
import type { Core } from "../job/core.ts";
import type { JobHost } from "../job/host.ts";
import { coreOf } from "../job/host.ts";
import { DeliveryError } from "../errors.ts";
import { RuntimeLifecycle } from "./lifecycle.ts";

export class Runtime extends RuntimeLifecycle {
  constructor(bus: Bus, clock: Clock, idGen: IdGen) {
    super(bus, clock, idGen);
  }

  protected answerSpent(error: DeliveryError, clock: Clock): void {
    const envelope = error.envelope;
    const requestId = correlationIdOf(envelope.header.address);
    if (
      envelope.header.address.kind !== "result" ||
      requestId === undefined ||
      !this.spent(envelope)
    ) {
      return;
    }
    const failure = makeEnvelope({
      topic: envelope.header.topic,
      payload: errPayload("delivery_failed"),
      from: envelope.header.from,
      idGen: this.idGen,
      cause: envelope.header.cause,
      kind: "result",
      correlationId: requestId,
    });
    let held = false;
    for (const host of this.hosts.values()) {
      if (
        host.resumeHeld(requestId, failure, this.bus, clock, this.idGen, () =>
          this.sync(coreOf(host)),
        )
      )
        held = true;
    }
    if (!held) this.bus.deliver(failure, this.agenda);
  }

  private spent(envelope: Envelope): boolean {
    for (const host of this.hosts.values()) {
      if (host.deliverySpent(envelope)) return true;
    }
    return false;
  }

  protected written(host: JobHost): Core {
    const core = coreOf(host);
    if (!core.signal.boundTo(this.agenda))
      throw new Error("runtime is the only writer");
    return core;
  }
}
