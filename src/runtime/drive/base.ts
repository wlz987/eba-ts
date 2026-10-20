import type { Caller } from "../../job/caller.ts";
import type { Runtime } from "../runtime.ts";
import { watchSpace } from "../../mail/inbox.ts";
import { SpaceKick, WakeTicket } from "../signals.ts";
import type { PendingWait } from "../signals.ts";

const attached = new WeakMap<Runtime, DriveBase>();

export abstract class DriveBase {

  protected runtime: Runtime;
  protected abstract runLoop(): Promise<void>;
  protected abstract failWaiters(error: Error): void;
  protected abstract release(): void;
  protected waiters!: Map<string, PendingWait>;
  protected stops!: Array<{ caller: Caller; rootId: string }>;
  protected wake!: WakeTicket | null;
  protected spaceKicks!: Map<number, SpaceKick>;
  protected ended!: boolean;
  protected fault!: Error | null;
  protected closed!: boolean;
  protected pumping!: boolean;
  protected releasing!: boolean;
  protected task: Promise<void> | null = null;

  constructor(runtime: Runtime) {
    this.runtime = runtime;
    this.reset();
  }

  protected reset(): void {
    this.waiters = new Map();
    this.stops = [];
    this.spaceKicks = new Map();
    this.wake = new WakeTicket();
    this.closed = false;
    this.pumping = false;
    this.releasing = false;
    this.fault = null;
    this.ended = false;
  }

  async start(): Promise<void> {
    if (this.task !== null && !this.ended)
      throw new Error("drive already running");
    const prior = attached.get(this.runtime);
    if (
      prior !== undefined &&
      prior !== this &&
      prior.task !== null &&
      !prior.ended
    ) {
      throw new Error("runtime already has a drive");
    }
    this.reset();
    this.runtime.bindWake(() => this.kick());
    attached.set(this.runtime, this);
    this.task = (async () => {
      try {
        await this.runLoop();
      } catch (error) {
        this.fault = error instanceof Error ? error : new Error(String(error));
        this.failWaiters(this.fault);
      } finally {
        this.ended = true;
        this.wake?.fire();
      }
    })();
  }

  async close(): Promise<void> {
    this.closed = true;
    this.wake?.fire();
    const task = this.task;
    this.runtime.bindWake(null);
    if (attached.get(this.runtime) === this) attached.delete(this.runtime);
    try {
      if (task !== null) await task;
    } finally {
      this.task = null;
      this.ended = false;
      this.failWaiters(new Error("drive closed"));
    }
    if (this.fault !== null) throw this.fault;
  }

  kick(): void {
    if (!this.pumping && !this.releasing) this.wake?.fire();
  }

  protected armSpace(caller: Caller): void {
    const inbox = caller.inbox;
    if (this.spaceKicks.has(inbox.id)) return;
    const kick = new SpaceKick(() => {
      this.spaceKicks.delete(inbox.id);
      this.wake?.fire();
    });
    this.spaceKicks.set(inbox.id, kick);
    watchSpace(inbox, kick);
  }

}
