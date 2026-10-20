import { TIMER, Signal } from "./signal.ts";
import type { AgendaLike } from "./signal.ts";
import { Deadlines } from "../time/deadlines.ts";
import { Schedule } from "../time/schedule.ts";
import { toError } from "../errors.ts";

export class Agenda implements AgendaLike {
  readonly signals = new Map<string, WeakRef<Signal>>();
  private ready = new Map<string, null>();
  private active = new Set<string>();
  private timers = new Deadlines<string>();
  private wake: (() => void) | null = null;
  private fault: Error | null = null;

  bindWake(wake: (() => void) | null): void {
    this.wake = wake;
    this.fault = null;
  }

  wakeFn(): void {
    const found = this.wake;
    if (found === null) return;
    try {
      found();
    } catch (error) {
      this.fault = toError(error);
    }
  }

  takeFault(): Error | null {
    const fault = this.fault;
    this.fault = null;
    return fault;
  }

  enqueue(actor: string): void {
    this.active.add(actor);
    this.ready.set(actor, null);
    this.wakeFn();
  }

  unqueue(actor: string): void {
    this.ready.delete(actor);
  }

  isReady(actor: string): boolean {
    return this.ready.has(actor);
  }

  get readySize(): number {
    return this.ready.size;
  }

  pop(): string | null {
    if (this.ready.size === 0) return null;
    const actor = this.ready.keys().next().value!;
    this.ready.delete(actor);
    return actor;
  }

  due(nowMs: number): void {
    while (true) {
      const actor = this.timers.peekDue(nowMs);
      if (actor === null) return;
      this.timers.remove(actor);
      const weak = this.signals.get(actor);
      if (weak === undefined) continue;
      const signal = weak.deref();
      if (signal !== undefined) signal.notify(TIMER);
    }
  }

  update(
    actor: string,
    deadline: number | null,
    active: boolean,
    ready: boolean,
  ): void {
    if (this.timers.at(actor) !== deadline) {
      this.timers.remove(actor);
      if (deadline !== null) this.timers.add(actor, deadline);
    }
    if (active) this.active.add(actor);
    else this.active.delete(actor);
    if (ready) this.enqueue(actor);
    else this.unqueue(actor);
  }

  remove(actor: string): void {
    this.unqueue(actor);
    this.active.delete(actor);
    this.timers.remove(actor);
    this.signals.delete(actor);
  }

  schedule(nowMs: number): Schedule {
    const deadline = this.timers.nextMs;
    return new Schedule(
      this.ready.size > 0 || (deadline !== null && deadline <= nowMs),
      deadline,
      this.active.size === 0,
    );
  }
}
