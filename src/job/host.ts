import { Envelope } from "../envelope/envelope.ts";
import type { ActorId } from "../envelope/ids.ts";
import type { Bus } from "../mail/bus.ts";
import type { Clock } from "../time/clock.ts";
import type { Receiver } from "../mail/inbox.ts";
import type { Job } from "./job.ts";
import { Core } from "./core.ts";
import { cores, coreOf } from "./host_core.ts";
export { coreOf } from "./host_core.ts";
import { HostView } from "./host_view.ts";
import { closeHost } from "./close.ts";

export interface HostOptions {
  actorId: ActorId | string;
  receiver: Receiver;
  patterns: readonly string[];
  requestTimeoutMs?: number | null;
  queueLimit?: number;
  activeLimit?: number;
  workBudget?: number;
  outboxLimit?: number;
}

export class JobHost extends HostView {
  constructor(options: HostOptions) {
    super();
    const hostRef = new WeakRef(this);
    const makeJob = (root: Envelope): Job => {
      const found = hostRef.deref();
      if (found === undefined) throw new Error("invalid JobHost");
      return found.makeJob(root);
    };
    const onRejected = (root: Envelope, reason: string): unknown => {
      const found = hostRef.deref();
      if (found === undefined) return undefined;
      return found.onRejected(root, reason);
    };
    cores.set(
      this,
      new Core({
        actorId: options.actorId,
        receiver: options.receiver,
        patterns: options.patterns,
        requestTimeoutMs: options.requestTimeoutMs ?? null,
        queueLimit: options.queueLimit ?? 512,
        activeLimit: options.activeLimit ?? 256,
        workBudget: options.workBudget ?? 64,
        outboxLimit: options.outboxLimit ?? 512,
        makeJob,
        onRejected,
      }),
    );
  }

  onStep(_clock: Clock): void {}
  close(bus: Bus): Envelope[] {
    const core = coreOf(this);
    if (core.signal.isBound)
      throw new Error("runtime owns host; use Runtime.close");
    const abandoned = closeHost(core, bus);
    core.abandoned = null;
    return abandoned;
  }
}
