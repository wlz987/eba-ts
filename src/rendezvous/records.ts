import type { Bus } from "../mail/bus.ts";
import { Subscription } from "../mail/bus.ts";
import type { Inbox } from "../mail/inbox.ts";
import { Deadlines, ELAPSED } from "../time/deadlines.ts";
import type { Envelope } from "../envelope/envelope.ts";
import { snapshotEnvelope } from "../envelope/envelope.ts";
import { EnvelopeBuildError } from "../envelope/errors.ts";
import type { EnvelopeId } from "../envelope/ids.ts";
import { nonnegativeInt } from "../time/numbers.ts";
import { Completion, resultBody, resultRequestId } from "../envelope/result.ts";
import { combineFailures } from "../errors.ts";
import { type Lease, type Releasable, type Route, mine, right } from "./lease.ts";
export { type Lease, type Releasable, matched } from "./lease.ts";

export class UnreadableResult extends Error {
  constructor() {
    super("unreadable result");
    this.name = "UnreadableResult";
  }
}

class Record {
  readonly topic: string;
  readonly cause: EnvelopeId;
  readonly continuation: object;
  readonly inbox: Inbox;
  constructor(
    topic: string,
    cause: EnvelopeId,
    continuation: object,
    inbox: Inbox,
  ) {
    this.topic = topic;
    this.cause = cause;
    this.continuation = continuation;
    this.inbox = inbox;
  }
}

export class Claim {
  readonly continuation: object;
  readonly completion: Completion;
  constructor(continuation: object, completion: Completion) {
    this.continuation = continuation;
    this.completion = completion;
  }
}

export class Records {
  private entries = new Map<EnvelopeId, Record>();
  private deadlines = new Deadlines();
  private routes = new Map<number, Map<string, Route>>();
  private stuck: Lease[] = [];

  get size(): number {
    return this.entries.size;
  }

  get nextDeadlineMs(): number | null {
    return this.deadlines.nextMs;
  }

  elapseOpen(): void {
    for (const requestId of [...this.entries.keys()]) {
      if (this.deadlines.at(requestId) === null) {
        this.deadlines.add(requestId, ELAPSED);
      }
    }
  }

  get unreleased(): number {
    return this.stuck.length;
  }

  continuation(requestId: EnvelopeId): object | null {
    const record = this.entries.get(requestId);
    return record === undefined ? null : record.continuation;
  }

  ids(): EnvelopeId[] {
    return [...this.entries.keys()];
  }

  submit(
    bus: Bus,
    inbox: Inbox,
    request: Envelope,
    continuation: object,
    deadlineMs: number | null,
  ): void {
    const snapshot = snapshotEnvelope(request);
    const header = snapshot.header;
    const address = header.address;
    if (address.kind !== "request") {
      throw new EnvelopeBuildError("expected a request with reply_to");
    }
    const replyTo = address.replyTo;
    if (this.entries.has(header.id)) {
      throw new EnvelopeBuildError(`duplicate request id: ${header.id}`);
    }
    if (deadlineMs !== null) nonnegativeInt(deadlineMs, "deadline_ms");
    if (inbox.isClosed) throw new Error("inbox closed");
    let routes = this.ensureRoutes(inbox.id);
    let route = routes.get(replyTo);
    if (
      route !== undefined &&
      route.count === 0 &&
      !route.lease.owner.active()
    ) {
      this.forget(route.lease);
      this.dropRoute(inbox.id, replyTo);
      route = undefined;
    }
    if (route === undefined) {
      routes = this.ensureRoutes(inbox.id);
      const frame = bus.openGrants();
      try {
        let granted: Releasable | null = null;
        try {
          granted = bus.subscribe(replyTo, inbox);
          route = {
            lease: { target: granted, owner: right(inbox.id, granted, frame) },
            count: 0,
            bus,
          };
        } catch (error) {
          this.abandon(granted, frame, inbox.id);
          if (routes.size === 0) this.routes.delete(inbox.id);
          throw error;
        }
      } finally {
        bus.closeGrants();
      }
    }
    route = { lease: route!.lease, count: route!.count + 1, bus: route!.bus };
    routes.set(replyTo, route);
    this.entries.set(
      header.id,
      new Record(replyTo, header.cause, continuation, inbox),
    );
    bus.holdWait(header.id);
    try {
      const delivered = bus.publish(snapshot);
      let due = deadlineMs;
      if (delivered === false && due === null) {
        due = ELAPSED;
      }
      if (due !== null) this.deadlines.add(header.id, due);
    } catch (error) {
      try {
        this.finish(header.id);
      } catch (secondary) {
        throw combineFailures(error, secondary);
      }
      throw error;
    }
  }

  takeResult(envelope: unknown): Claim | null {
    let snapshot: Envelope;
    try {
      snapshot = snapshotEnvelope(envelope);
    } catch (error) {
      if (error instanceof EnvelopeBuildError || error instanceof TypeError) {
        throw new UnreadableResult();
      }
      throw error;
    }
    const requestId = resultRequestId(snapshot);
    const record = requestId === null ? undefined : this.entries.get(requestId);
    if (
      requestId === null ||
      record === undefined ||
      record.topic !== snapshot.header.topic ||
      record.cause !== snapshot.header.cause
    ) {
      return null;
    }
    let body: ReturnType<typeof resultBody>;
    try {
      body = resultBody(snapshot);
    } catch {
      throw new UnreadableResult();
    }
    if (body === null) throw new UnreadableResult();
    const claimed = this.finish(requestId, { claim: true });
    if (claimed === null) return null;
    return new Claim(claimed.continuation, new Completion(requestId, body));
  }

  takeDue(nowMs: number): Claim | null {
    nonnegativeInt(nowMs, "clock_ms");
    const requestId = this.deadlines.peekDue(nowMs);
    if (requestId === null) return null;
    const record = this.finish(requestId as EnvelopeId, { claim: true });
    if (record === null) return null;
    return new Claim(
      record.continuation,
      new Completion(requestId as EnvelopeId, null),
    );
  }

  recover(): void {
    this.retry();
  }

  finish(
    requestId: EnvelopeId,
    options: { claim?: boolean } = {},
  ): Record | null {
    const claim = options.claim ?? false;
    this.retry();
    const record = this.entries.get(requestId);
    if (record === undefined) return null;
    const routes = this.routes.get(record.inbox.id)!;
    const route = routes.get(record.topic)!;
    let closed = true;
    if (route.count === 1) {
      const before = route.bus.subscriptionCount(record.topic, record.inbox);
      try {
        route.lease.target.close();
      } catch (error) {
        if (!claim) throw error;
        const released =
          !route.lease.owner.active() ||
          route.bus.subscriptionCount(record.topic, record.inbox) < before;
        closed = released;
        if (!closed) this.remember(route.lease);
      }
    }
    this.entries.delete(requestId);
    this.deadlines.remove(requestId);
    route.bus.releaseWait(requestId);
    const remaining = route.count - 1;
    if (remaining === 0 && closed) {
      this.dropRoute(record.inbox.id, record.topic);
      this.forget(route.lease);
    } else if (remaining === 0) {
      routes.set(record.topic, {
        lease: route.lease,
        count: 0,
        bus: route.bus,
      });
    } else {
      routes.set(record.topic, {
        lease: route.lease,
        count: remaining,
        bus: route.bus,
      });
    }
    if (claim) this.retry();
    return record;
  }

  private remember(lease: Lease): void {
    if (this.stuck.every((item) => item.target !== lease.target))
      this.stuck.push(lease);
  }

  private forget(lease: Lease): void {
    this.stuck = this.stuck.filter((item) => item.target !== lease.target);
  }

  private abandon(
    granted: Releasable | null,
    frame: Array<[number, Subscription]>,
    inboxId: number,
  ): void {
    const natives = mine(inboxId, frame);
    const targets: Releasable[] = [];
    if (granted !== null) targets.push(granted);
    for (const sub of natives) {
      if (targets.every((item) => item !== sub)) targets.push(sub);
    }
    for (const target of targets) {
      if (typeof target.close !== "function") continue;
      try {
        target.close();
      } catch {
        let owner: Subscription;
        try {
          owner = right(inboxId, target, frame);
        } catch {
          continue;
        }
        if (!owner.active()) continue;
        this.remember({ target, owner });
      }
    }
  }

  private ensureRoutes(inboxId: number): Map<string, Route> {
    let routes = this.routes.get(inboxId);
    if (routes === undefined) {
      routes = new Map();
      this.routes.set(inboxId, routes);
    }
    return routes;
  }

  private findRoute(target: Releasable): [number, string] | null {
    for (const [inboxId, routes] of this.routes) {
      for (const [topic, route] of routes) {
        if (route.lease.target === target) return [inboxId, topic];
      }
    }
    return null;
  }

  private dropRoute(inboxId: number, topic: string): void {
    const routes = this.routes.get(inboxId);
    if (routes === undefined) return;
    routes.delete(topic);
    if (routes.size === 0) this.routes.delete(inboxId);
  }

  private retry(): void {
    const kept: Lease[] = [];
    for (const lease of this.stuck) {
      const found = this.findRoute(lease.target);
      if (!lease.owner.active()) {
        if (found !== null) {
          const [inboxId, topic] = found;
          if (this.routes.get(inboxId)!.get(topic)!.count === 0) {
            this.dropRoute(inboxId, topic);
          }
        }
        continue;
      }
      if (found !== null) {
        const route = this.routes.get(found[0])!.get(found[1])!;
        if (route.count > 0) {
          kept.push(lease);
          continue;
        }
      }
      try {
        lease.target.close();
      } catch {
        kept.push(lease);
        continue;
      }
      if (lease.owner.active()) {
        kept.push(lease);
        continue;
      }
      if (found !== null) this.dropRoute(found[0], found[1]);
    }
    this.stuck = kept;
  }
}
