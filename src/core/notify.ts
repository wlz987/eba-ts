import { Deadlines, safeDeadline, Schedule } from "#src/core/time.js";
import { toError } from "#src/core/errors.js";
import { lookupKeys } from "#src/core/pattern.js";
import type { Pattern, Wildcard } from "#src/core/pattern.js";

// --- signal ----------------------------------------------------------------

export const READ = 1;
export const TIMER = 2;
export const WORK = 4;
export const DELIVERY = 8;


export interface AgendaLike {
  bindSignal(actor: string, signal: WeakRef<Signal>): void;
  unbindSignal?(actor: string): void;
  hasSignal(actor: string): boolean;
  enqueue(actor: string): void;
  unqueue(actor: string): void;
}

export class Signal {
  flags = 0;
  #owner: WeakRef<AgendaLike> | null = null;
  #id: string | null = null;

  get actor(): string | null {
    return this.#id;
  }
  get isBound(): boolean {
    return this.#owner !== null && this.#owner.deref() !== undefined;
  }
  get handed(): boolean {
    return this.#owner !== null;
  }

  boundTo(owner: AgendaLike): boolean {
    return this.#owner !== null && this.#owner.deref() === owner;
  }

  has(flag: number): boolean {
    return (this.flags & flag) !== 0;
  }

  notify(flag: number): void {
    this.flags = (this.flags | flag) >>> 0;
    if (this.#id === null || this.#owner === null) return;
    const owner = this.#owner.deref();
    if (owner !== undefined) owner.enqueue(this.#id);
  }

  clear(flag: number): void {
    this.flags = (this.flags & ~flag) >>> 0;
    if (this.flags !== 0 || this.#id === null || this.#owner === null) return;
    const owner = this.#owner.deref();
    if (owner !== undefined) owner.unqueue(this.#id);
  }

  set(flag: number, ready: boolean): void {
    if (ready) this.notify(flag);
    else this.clear(flag);
  }

  bind(owner: AgendaLike, actor: string): void {
    if (this.#owner !== null && this.#id !== null) {
      const prior = this.#owner.deref();
      if (prior !== undefined && prior !== owner) prior.unbindSignal?.(this.#id);
    }
    this.#owner = new WeakRef(owner);
    this.#id = actor;
    owner.bindSignal(actor, new WeakRef(this));
    if (this.flags) owner.enqueue(actor);
  }

  detach(): void {
    // Release the owner's side of the binding too: an agenda keeps a signal
    // map keyed by actor, and a detached signal must not leave a stale entry.
    if (this.#owner !== null && this.#id !== null) {
      const prior = this.#owner.deref();
      if (prior !== undefined) prior.unbindSignal?.(this.#id);
    }
    this.#owner = null;
    this.#id = null;
  }
}

// --- watchers --------------------------------------------------------------

export function setCandidate(
  candidates: Record<string, null>,
  topic: string,
): void {
  candidates[topic] = null;
}

const hooks = new WeakMap<Interest, () => void>();

/** Detach an interest from every watcher set (internal teardown). */
export function disposeInterest(interest: Interest): void {
  // Detach by snapshot: clearing the set *before* calling `remove` lets an
  // `onEmpty` hook legitimately re-watch this interest — the fresh owner link
  // it adds survives, instead of being wiped by a trailing `clear()`.
  const owners = (interest.$owners);
  const prior = [...owners];
  owners.clear();
  for (const owner of prior) owner.remove(interest);
}

export class Interest {
  readonly key: object = {};
  $owners = new Set<Watchers>();
  readonly signal: Signal;
  readonly candidates: Record<string, null>;
  readonly topic: string;

  constructor(
    signal: Signal,
    candidates: Record<string, null>,
    topic: string,
    extra: Record<string, unknown> = {},
  ) {
    this.signal = signal;
    this.candidates = candidates;
    this.topic = topic;
    Object.assign(this, extra);
  }

  setOnNotify(hook: () => void): void {
    hooks.set(this, hook);
  }

  notify(): void {
    const hook = hooks.get(this);
    if (hook !== undefined) {
      hook();
      return;
    }
    setCandidate(this.candidates, this.topic);
    this.signal.notify(DELIVERY);
  }

  dispose(): void {
    disposeInterest(this);
  }

}

export class Watchers {
  #waiting = new Map<object, WeakRef<Interest>>();
  #onEmpty: (() => void) | null;

  constructor(onEmpty?: () => void) {
    this.#onEmpty = onEmpty ?? null;
  }

  watch(interest: Interest): void {
    this.#waiting.set(interest.key, new WeakRef(interest));
    (interest.$owners).add(this);
  }

  live(): Interest[] {
    const found: Interest[] = [];
    for (const weak of this.#waiting.values()) {
      const interest = weak.deref();
      if (interest !== undefined) found.push(interest);
    }
    return found;
  }

  get watched(): number {
    return this.#waiting.size;
  }

  remove(interest: Interest): void {
    if (!this.#waiting.delete(interest.key)) return;
    // A watch is a two-way link: dropping it must clear the interest's owner
    // set too, or `ownersLen` reports a watch that no longer exists.
    (interest.$owners).delete(this);
    if (this.#waiting.size === 0 && this.#onEmpty !== null) this.#onEmpty();
  }

  notify(): void {
    const waiting = [...this.#waiting.values()];
    if (waiting.length === 0) return;
    this.#waiting.clear();
    // Detach every owner link BEFORE the empty hook: the hook may re-watch one
    // of these interests, and that fresh link must survive.
    for (const weak of waiting) {
      const interest = weak.deref();
      if (interest !== undefined) (interest.$owners).delete(this);
    }
    if (this.#onEmpty !== null) this.#onEmpty();
    let error: unknown = null;
    for (const weak of waiting) {
      const interest = weak.deref();
      if (interest === undefined) continue;
      try {
        interest.notify();
      } catch (caught) {
        if (error === null) error = caught;
      }
    }
    if (error !== null) throw error;
  }
}

type IndexMap = Map<string, Watchers>;

export class Changes {
  #indexes: Record<Wildcard, IndexMap> = {
    none: new Map(),
    star: new Map(),
    globstar: new Map(),
  };

  watch(topic: string, interest: Interest): void {
    for (const [kind, key] of lookupKeys(topic.split("."))) {
      this.watchKey(kind, key, interest);
    }
  }

  notify(pattern: Pattern): void {
    const watchers = this.#indexes[pattern.wildcard].get(
      pattern.literal.join("."),
    );
    if (watchers !== undefined) watchers.notify();
  }

  private watchKey(kind: Wildcard, key: string, interest: Interest): void {
    const index = this.#indexes[kind];
    let source = index.get(key);
    if (source === undefined) {
      source = new Watchers(() => {
        if (index.get(key) === source) index.delete(key);
      });
      index.set(key, source);
    }
    source.watch(interest);
  }
}

// --- agenda ----------------------------------------------------------------

export class Agenda implements AgendaLike {
  #signals = new Map<string, WeakRef<Signal>>();
  #ready = new Map<string, null>();
  #active = new Set<string>();
  #timers = new Deadlines<string>();
  #wake: (() => void) | null = null;
  #fault: Error | null = null;

  bindSignal(actor: string, signal: WeakRef<Signal>): void {
    this.#signals.set(actor, signal);
  }

  unbindSignal(actor: string): void {
    this.#signals.delete(actor);
    this.unqueue(actor);
    this.#active.delete(actor);
  }

  hasSignal(actor: string): boolean {
    return this.#signals.has(actor);
  }

  bindWake(wake: (() => void) | null): void {
    this.#wake = wake;
    this.#fault = null;
  }

  wakeFn(): void {
    const found = this.#wake;
    if (found === null) return;
    try {
      found();
    } catch (error) {
      this.#fault = toError(error);
    }
  }

  takeFault(): Error | null {
    const fault = this.#fault;
    this.#fault = null;
    return fault;
  }

  enqueue(actor: string): void {
    this.#active.add(actor);
    this.#ready.set(actor, null);
    this.wakeFn();
  }

  unqueue(actor: string): void {
    this.#ready.delete(actor);
  }

  get readySize(): number {
    return this.#ready.size;
  }

  get nextDeadlineMs(): number | null {
    return this.#timers.nextMs;
  }

  pop(): string | null {
    if (this.#ready.size === 0) return null;
    const actor = this.#ready.keys().next().value!;
    this.#ready.delete(actor);
    return actor;
  }

  due(nowMs: number): void {
    while (true) {
      const actor = this.#timers.peekDue(nowMs);
      if (actor === null) return;
      this.#timers.remove(actor);
      const weak = this.#signals.get(actor);
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
    const due = deadline === null ? null : safeDeadline(deadline);
    if (this.#timers.at(actor) !== due) {
      this.#timers.remove(actor);
      if (due !== null) this.#timers.add(actor, due);
    }
    if (active) this.#active.add(actor);
    else this.#active.delete(actor);
    if (ready) this.enqueue(actor);
    else this.unqueue(actor);
  }

  remove(actor: string): void {
    this.#signals.get(actor)?.deref()?.detach();
    this.unqueue(actor);
    this.#active.delete(actor);
    this.#timers.remove(actor);
    this.#signals.delete(actor);
  }

  schedule(nowMs: number): Schedule {
    const deadline = this.#timers.nextMs;
    return new Schedule(
      this.#ready.size > 0 || (deadline !== null && deadline <= nowMs),
      deadline,
      this.#active.size === 0,
    );
  }
}
