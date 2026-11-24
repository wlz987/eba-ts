import type { Bus } from "#src/core/mail/bus.js";
import type { Envelope, EnvelopeId } from "#src/core/envelope.js";
import type { Inbox } from "#src/core/mail/inbox.js";
import type { LeaseClaim } from "#src/core/rendezvous/records.js";
import { LeaseRecords } from "#src/core/rendezvous/records.js";

/** Which job a request belongs to, and which stage it will settle. */
export class LeaseContinuation {
  readonly owner: EnvelopeId;
  readonly stage: string;
  constructor(owner: EnvelopeId, stage: string) {
    this.owner = owner;
    this.stage = stage;
  }
}

export class LeaseRegistry {
  /**
   * The lease book, held in `#` fields so JS cannot rewrite ownership, routes
   * or the pending-cancel list.
   */
  #records = new LeaseRecords();
  #owners = new Map<EnvelopeId, Set<EnvelopeId>>();
  #cancelled: EnvelopeId[] = [];
  get size(): number {
    return this.#records.size;
  }

  get nextDeadlineMs(): number | null {
    return this.#records.nextDeadlineMs;
  }

  get unreleased(): number {
    return this.#records.unreleased;
  }

  count(owner: EnvelopeId): number {
    return this.#owners.get(owner)?.size ?? 0;
  }

  holds(requestId: EnvelopeId): boolean {
    return this.#records.continuation(requestId) !== null;
  }

  submit(
    bus: Bus,
    inbox: Inbox,
    request: Envelope,
    owner: EnvelopeId,
    stage: string,
    deadlineMs: number | null,
  ): void {
    const continuation = new LeaseContinuation(owner, stage);
    try {
      this.#records.submit(bus, inbox, request, continuation, deadlineMs);
    } finally {
      if (this.#records.continuation(request.header.id) === continuation) {
        let owned = this.#owners.get(owner);
        if (owned === undefined) {
          owned = new Set();
          this.#owners.set(owner, owned);
        }
        owned.add(request.header.id);
      }
    }
  }

  takeResult(envelope: Envelope): LeaseClaim | null {
    return this.#claim(this.#records.takeResult(envelope));
  }

  expireDue(nowMs: number): LeaseClaim | null {
    const expired = this.#records.expireDue(nowMs);
    if (expired !== null && expired.continuation instanceof LeaseContinuation) {
      // The lease expired *without an answer*: the caller has given up on this
      // request. `finish` already deleted the lease record, so the owner index
      // no longer knows the request id — but the downstream worker still has a
      // live job for it. Nobody else holds that id, so it could never be
      // revoked: the worker job would live forever, its `onFinished` would never
      // run (violating I2), and the worker host would stay permanently
      // non-quiescent with no armed deadline. Queue the id for cancellation so
      // `revoke` propagates to the worker on this turn.
      this.#cancelled.push(expired.completion.requestId);
    }
    return this.#claim(expired);
  }

  elapseOpen(): void {
    this.#records.elapseOpen();
  }

  recover(): void {
    this.#records.recover();
  }

  cancel(owner: EnvelopeId): void {
    const owned = this.#owners.get(owner);
    for (const requestId of owned ? [...owned] : []) {
      this.#records.finish(requestId);
      this.#forget(owner, requestId);
      this.#cancelled.push(requestId);
    }
  }

  takeCancelled(): EnvelopeId[] {
    const found = this.#cancelled;
    this.#cancelled = [];
    return found;
  }

  #claim(claim: LeaseClaim | null): LeaseClaim | null {
    if (claim === null) return null;
    const continuation = claim.continuation;
    if (continuation instanceof LeaseContinuation) {
      this.#forget(continuation.owner, claim.completion.requestId);
    }
    return claim;
  }

  #forget(owner: EnvelopeId, requestId: EnvelopeId): void {
    const owned = this.#owners.get(owner);
    if (owned === undefined) return;
    owned.delete(requestId);
    if (owned.size === 0) this.#owners.delete(owner);
  }
}
