import type { Envelope, IdGen } from "#src/core/envelope.js";
import type { Bus } from "#src/core/mail/bus.js";
import type { Clock } from "#src/core/time.js";
import type { JobHost } from "#src/core/job/host.js";
import { coreOf } from "#src/core/job/host.js";
import { Agenda } from "#src/core/notify.js";
import { closeCount } from "#src/core/mail/inbox.js";

export type RuntimeState = "running" | "draining" | "closing" | "closed";

/** The runtime's own state: hosts, agenda, turn bookkeeping, lifecycle verdict. */

/** Retain abandoned roots from a close that failed; the next close returns them. */
export function stashRoots(runtime: RuntimeBase, roots: Envelope[]): void {
  runtime.$stashedRoots.push(...roots);
}

export function takeStashed(runtime: RuntimeBase): Envelope[] {
  const kept = runtime.$stashedRoots;
            runtime.$stashedRoots = [];
            return kept;
}

export function takeFlushes(runtime: RuntimeBase): Promise<unknown>[] {
  const pending = runtime.$flushes;
            runtime.$flushes = [];
            return pending;
}

/** The live flush collector `closeHost` appends to (internal). */
export function flushSink(runtime: RuntimeBase): Promise<unknown>[] {
  return runtime.$flushes;
}

/** Release the bus claim; internal so application code cannot drive it. */
export function releaseClaimOf(runtime: RuntimeBase): void {
  const token = runtime.$claim;
  runtime.$claim = null;
  if (token === null) return;
  runtime.$bus.release(token);
}

export function setHost(runtime: RuntimeBase, actor: string, host: JobHost): void {
  runtime.$hosts.set(actor, host);
}

export function deleteHost(runtime: RuntimeBase, actor: string): void {
  runtime.$hosts.delete(actor);
}

/** Enter a turn: sets the reentrancy guard. */
export function markBusy(runtime: RuntimeBase): void {
  runtime.$busy = true;
}

/** Leave a turn: clears the reentrancy guard. */
export function markIdle(runtime: RuntimeBase): void {
  runtime.$busy = false;
}

export function markReadingClock(runtime: RuntimeBase): void {
  runtime.$readingClock = true;
}

export function markClockRead(runtime: RuntimeBase): void {
  runtime.$readingClock = false;
}

export function pinTurn(runtime: RuntimeBase, nowMs: number): number {
  const delta = runtime.$hasLastNow
                ? Math.max(0, nowMs - runtime.$lastNowMs)
                : 0;
            runtime.$lastNowMs = nowMs;
            runtime.$hasLastNow = true;
            runtime.$observed = nowMs;
            return delta;
}

/** Forget the pinned instant (the turn ended). */
export function clearObserved(runtime: RuntimeBase): void {
  runtime.$observed = null;
}

export function noteCloses(runtime: RuntimeBase, count: number): void {
  runtime.$noticedCloses = count;
}

/** Start draining: no new hosts are admitted, in-flight work finishes. */
export function markDraining(runtime: RuntimeBase): void {
  runtime.$state = "draining";
}

export function markClosing(runtime: RuntimeBase): void {
  runtime.$state = "closing";
}

export function markClosed(runtime: RuntimeBase): void {
  runtime.$state = "closed";
}

export function agendaOf(runtime: RuntimeBase): Agenda {
  return runtime.$agenda;
}

/** The runtime's **live** host registry, for internal bookkeeping. */
export function hostsOf(runtime: RuntimeBase): Map<string, JobHost> {
  return runtime.$hosts;
}

/**
 * Record the bus-claim capability `Bus.claim` answered. `Runtime`'s constructor
 * is the only caller; see the note at the `$claim` field for why the token must
 * never be replaced.
 */
export function adoptClaim(runtime: RuntimeBase, token: symbol): void {
  runtime.$claim = token;
}

export class RuntimeBase {
  $hosts = new Map<string, JobHost>();
  readonly $agenda = new Agenda();
  $busy = false;
  $readingClock = false;
  $observed: number | null = null;
  /**
   * Wall-clock instant charged on the previous turn; drives lease budget
   * consumption. `observed` is a per-turn pin, so it cannot be the reference.
   */
  $lastNowMs = 0;
  $hasLastNow = false;
  $state: RuntimeState = "running";
  $noticedCloses = closeCount();
  readonly $bus: Bus;
  readonly clock: Clock;
  readonly idGen: IdGen;
  $claim: symbol | null = null;
  /** Abandoned roots retained when a close failed, returned by the next close. */
  $stashedRoots: Envelope[] = [];
  /** Final flushes started by `close()`/`closeAsync()`, awaited by `closeAsync()`. */
  $flushes: Promise<unknown>[] = [];

  /**
   * Hand the bus claim back exactly once.
   *
   * Idempotent: `close()` and a retired runtime both route through here, and a
   * second call must not release a claim this runtime no longer holds. Kept on
   * the base class (not `Runtime`) because `RuntimeLifecycle.close()` — an
   * *ancestor* of `Runtime` — is the caller.
   */

  constructor(bus: Bus, clock: Clock, idGen: IdGen) {
    this.$bus = bus;
    this.clock = clock;
    this.idGen = idGen;
  }

  get bus(): Bus {
    return this.$bus;
  }
  /** Admitted hosts, keyed by actor id */
  get hosts(): ReadonlyMap<string, JobHost> {
    return this.$hosts;
  }
  /** The scheduler's agenda */
  get agenda(): Agenda {
    return this.$agenda;
  }
  /** True while a turn is executing (the reentrancy guard). */
  get busy(): boolean {
    return this.$busy;
  }
  /** True while the clock is being observed (guards reentrant reads). */
  get readingClock(): boolean {
    return this.$readingClock;
  }
  /** The instant pinned for the current turn, or `null` between turns. */
  get observed(): number | null {
    return this.$observed;
  }

  get state(): RuntimeState {
    return this.$state;
  }
  get noticedCloses(): number {
    return this.$noticedCloses;
  }

  owns(host: JobHost): boolean {
    const core = coreOf(host);
    return this.$hosts.get(String(core.sub.actorId)) === host;
  }

}

export function bindWakeOf(runtime: RuntimeBase, wake: (() => void) | null): void {
  agendaOf(runtime).bindWake(wake);
}
