import type { Bus } from "../mail/bus.ts";
import type { Clock } from "../time/clock.ts";
import type { Envelope } from "../envelope/envelope.ts";
import type { ActorId } from "../envelope/ids.ts";
import type { Completion } from "../envelope/result.ts";
import { makeEnvelope } from "../envelope/envelope.ts";
import type { IdGen } from "../envelope/idgen.ts";
import { correlationIdOf } from "../envelope/header.ts";
import { errPayload } from "../envelope/result.ts";
import type { Core } from "../job/core.ts";
import type { JobHost } from "../job/host.ts";
import { coreOf } from "../job/host.ts";
import { DeliveryError } from "../errors.ts";
import { Caller } from "../job/caller.ts";
import { Inbox } from "../mail/inbox.ts";
import { Pending } from "./signals.ts";
import { RuntimeLifecycle } from "./lifecycle.ts";

export class Runtime extends RuntimeLifecycle {
  private frontCaller: Caller | null = null;

  constructor(bus: Bus, clock: Clock, idGen: IdGen) {
    super(bus, clock, idGen);
  }

  ask(
    topic: string,
    payload: unknown,
    from: ActorId | string = "caller",
    timeoutMs?: number | null,
  ): Pending {
    if (this.frontCaller === null) {
      const caller = new Caller(new Inbox(8).receiver(), this.idGen, 256, "sys.ask");
      this.add(caller);
      this.frontCaller = caller;
    }
    let resolve!: (completion: Completion) => void;
    const ready = new Promise<Completion>((done) => {
      resolve = done;
    });
    const envelope = this.frontCaller.admit({
      topic,
      payload,
      from,
      timeoutMs: timeoutMs ?? null,
      pay: (completion) => resolve(completion),
    });
    return new Pending(envelope, ready);
  }

  tell(topic: string, payload: unknown, from: ActorId | string = "caller"): void {
    this.bus.publish(
      makeEnvelope({ topic, payload, from, idGen: this.idGen, kind: "event" }),
    );
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
