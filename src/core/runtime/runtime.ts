import type { Bus } from "#src/core/mail/bus.js";
import type { Clock } from "#src/core/time.js";
import type { Envelope, ActorId, Completion, IdGen } from "#src/core/envelope.js";
import { makeEnvelope } from "#src/core/envelope.js";
import { coreOf } from "#src/core/job/host.js";
import { hostsOf, adoptClaim } from "#src/core/runtime/base.js";
import { Caller } from "#src/core/job/caller.js";
import { Inbox } from "#src/core/mail/inbox.js";
import { Pending } from "#src/core/runtime/signals.js";
import { RuntimeLifecycle } from "#src/core/runtime/lifecycle.js";

export interface HostStats {
  active: number;
  held: number;
  queued: number;
  pending: number;
  rejected: number;
  pendingCancels: number;
  mailbox: number;
}

export interface RuntimeStats {
  hosts: number;
  active: number;
  held: number;
  queued: number;
  pending: number;
  rejected: number;
  pendingCancels: number;
  mailbox: number;
  hostsDetail: Record<string, HostStats>;
}

export class Runtime extends RuntimeLifecycle {
  /**
   * The lazily-built caller that fronts `ask`/`tell`. Sealed as `#` so a stray
   * `runtime.frontCaller = x` cannot redirect every call to an inbox the runtime
   * never registered, leaving replies in a box nobody drains.
   */
  #frontCaller: Caller | null = null;

  constructor(bus: Bus, clock: Clock, idGen: IdGen) {
    super(bus, clock, idGen);
    // that symbol, so the claim cannot be freed by a caller who merely holds the
    // runtime (which `bus.owner` hands back). See `RuntimeBase.#claim`.
    adoptClaim(this, bus.claim(this));
  }

  stats(): RuntimeStats {
    let active = 0;
    let held = 0;
    let queued = 0;
    let pending = 0;
    let rejected = 0;
    let pendingCancels = 0;
    let mailbox = 0;
    const hostsDetail: Record<string, HostStats> = {};
    for (const host of hostsOf(this).values()) {
      const core = coreOf(host);
      const detail: HostStats = {
        active: core.jobs.size,
        held: core.held.size,
        queued: core.queue.size,
        pending: core.leases.size,
        rejected: core.rejectedCount,
        pendingCancels: core.cancelPending.size,
        mailbox: core.mailbox.size,
      };
      hostsDetail[String(core.sub.actorId)] = detail;
      active += detail.active;
      held += detail.held;
      queued += detail.queued;
      pending += detail.pending;
      rejected += detail.rejected;
      pendingCancels += detail.pendingCancels;
      mailbox += detail.mailbox;
    }
    return {
      hosts: this.hosts.size,
      active,
      held,
      queued,
      pending,
      rejected,
      pendingCancels,
      mailbox,
      hostsDetail,
    };
  }

  ask(
    topic: string,
    payload: unknown,
    from: ActorId | string = "caller",
    timeoutMs?: number | null,
  ): Pending {
    let resolve!: (completion: Completion) => void;
    const ready = new Promise<Completion>((done) => {
      resolve = done;
    });
    const envelope = this.#front().admit({
      topic,
      payload,
      from,
      timeoutMs: timeoutMs ?? null,
      pay: (completion) => resolve(completion),
    });
    return new Pending(envelope, ready);
  }

  askWith(
    topic: string,
    payload: unknown,
    pay: (completion: Completion) => void,
    from: ActorId | string = "caller",
    timeoutMs?: number | null,
  ): Envelope {
    // Hand the completion to pay; it runs once inside a turn and must not re-enter the runtime.
    return this.#front().admit({
      topic,
      payload,
      from,
      timeoutMs: timeoutMs ?? null,
      pay,
    });
  }

  #front(): Caller {
    if (this.#frontCaller === null) {
      // front inbox matches the caller active limit
      const caller = new Caller(new Inbox(256).receiver(), this.idGen, 256, "sys.ask");
      this.add(caller);
      this.#frontCaller = caller;
    }
    return this.#frontCaller;
  }

  tell(topic: string, payload: unknown, from: ActorId | string = "caller"): void {
    // A retired runtime must not write into a bus that may already belong to a
    // new runtime: `close()` releases the claim, so this old handle would
    // otherwise inject events into somebody else's turn.
    if (this.state === "closing" || this.state === "closed")
      throw new Error("runtime is closed");
    // Fire-and-forget: `tell` may drop and never blocks, so route through the
    // best-effort fanout (full target inboxes are skipped, not raised).
    this.bus.tell(
      makeEnvelope({ topic, payload, from, idGen: this.idGen, kind: "event" }),
    );
  }

}
