import type { Bus } from "../mail/bus.ts";
import type { Envelope } from "../envelope/envelope.ts";
import type { EnvelopeId } from "../envelope/ids.ts";
import type { Inbox } from "../mail/inbox.ts";
import type { Claim } from "../rendezvous/records.ts";
import { Records } from "../rendezvous/records.ts";

export class Continuation {
  readonly owner: EnvelopeId;
  readonly stage: string;
  constructor(owner: EnvelopeId, stage: string) {
    this.owner = owner;
    this.stage = stage;
  }
}

export class RequestBook {
  private records = new Records();
  private owners = new Map<EnvelopeId, Set<EnvelopeId>>();
  private cancelled: EnvelopeId[] = [];

  get size(): number {
    return this.records.size;
  }

  get nextDeadlineMs(): number | null {
    return this.records.nextDeadlineMs;
  }

  get unreleased(): number {
    return this.records.unreleased;
  }

  count(owner: EnvelopeId): number {
    return this.owners.get(owner)?.size ?? 0;
  }

  holds(requestId: EnvelopeId): boolean {
    return this.records.continuation(requestId) !== null;
  }

  submit(
    bus: Bus,
    inbox: Inbox,
    request: Envelope,
    owner: EnvelopeId,
    stage: string,
    deadlineMs: number | null,
  ): void {
    const continuation = new Continuation(owner, stage);
    try {
      this.records.submit(bus, inbox, request, continuation, deadlineMs);
    } finally {
      if (this.records.continuation(request.header.id) === continuation) {
        let owned = this.owners.get(owner);
        if (owned === undefined) {
          owned = new Set();
          this.owners.set(owner, owned);
        }
        owned.add(request.header.id);
      }
    }
  }

  takeResult(envelope: Envelope): Claim | null {
    return this.claim(this.records.takeResult(envelope));
  }

  takeDue(nowMs: number): Claim | null {
    return this.claim(this.records.takeDue(nowMs));
  }

  elapseOpen(): void {
    this.records.elapseOpen();
  }

  recover(): void {
    this.records.recover();
  }

  cancel(owner: EnvelopeId): void {
    const owned = this.owners.get(owner);
    for (const requestId of owned ? [...owned] : []) {
      this.records.finish(requestId);
      this.forget(owner, requestId);
      this.cancelled.push(requestId);
    }
  }

  takeCancelled(): EnvelopeId[] {
    const found = this.cancelled;
    this.cancelled = [];
    return found;
  }

  private claim(claim: Claim | null): Claim | null {
    if (claim === null) return null;
    const continuation = claim.continuation;
    if (continuation instanceof Continuation) {
      this.forget(continuation.owner, claim.completion.requestId);
    }
    return claim;
  }

  private forget(owner: EnvelopeId, requestId: EnvelopeId): void {
    const owned = this.owners.get(owner);
    if (owned === undefined) return;
    owned.delete(requestId);
    if (owned.size === 0) this.owners.delete(owner);
  }
}
