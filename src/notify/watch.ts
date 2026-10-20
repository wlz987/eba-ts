import { DELIVERY, Signal } from "./signal.ts";
import { lookupKeys } from "../pattern/pattern.ts";
import type { Pattern, Wildcard } from "../pattern/pattern.ts";

export class Interest {
  readonly key: object = {};
  readonly owners = new Set<Watchers>();
  readonly signal: Signal;
  readonly candidates: Record<string, null>;
  readonly topic: string;
  constructor(signal: Signal, candidates: Record<string, null>, topic: string) {
    this.signal = signal;
    this.candidates = candidates;
    this.topic = topic;
  }
  notify(): void {
    this.candidates[this.topic] = null;
    this.signal.notify(DELIVERY);
  }
  dispose(): void {
    for (const owner of [...this.owners]) owner.remove(this);
    this.owners.clear();
  }
}

export class Watchers {
  private waiting = new Map<object, WeakRef<Interest>>();
  private onEmpty: (() => void) | null;
  constructor(onEmpty?: () => void) {
    this.onEmpty = onEmpty ?? null;
  }
  watch(interest: Interest): void {
    this.waiting.set(interest.key, new WeakRef(interest));
    interest.owners.add(this);
  }
  live(): Interest[] {
    const found: Interest[] = [];
    for (const weak of this.waiting.values()) {
      const interest = weak.deref();
      if (interest !== undefined) found.push(interest);
    }
    return found;
  }

  remove(interest: Interest): void {
    this.waiting.delete(interest.key);
    if (this.waiting.size === 0 && this.onEmpty !== null) this.onEmpty();
  }
  notify(): void {
    const waiting = [...this.waiting.values()];
    this.waiting.clear();
    if (this.onEmpty !== null) this.onEmpty();
    let error: unknown = null;
    for (const weak of waiting) {
      const interest = weak.deref();
      if (interest === undefined) continue;
      interest.owners.delete(this);
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
  private indexes: Record<Wildcard, IndexMap> = {
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
    const watchers = this.indexes[pattern.wildcard].get(
      pattern.literal.join("."),
    );
    if (watchers !== undefined) watchers.notify();
  }

  keepsKey(kind: Wildcard, key: string): boolean {
    return this.indexes[kind].has(key);
  }

  private watchKey(kind: Wildcard, key: string, interest: Interest): void {
    const index = this.indexes[kind];
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
