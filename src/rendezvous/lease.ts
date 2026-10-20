import type { Bus } from "../mail/bus.ts";
import { Subscription } from "../mail/bus.ts";

export interface Releasable {
  close(): boolean;
}

export interface Lease {
  target: Releasable;
  owner: Subscription;
}

export interface Route {
  lease: Lease;
  count: number;
  bus: Bus;
}

export function mine(
  inboxId: number,
  frame: Array<[number, Subscription]>,
): Subscription[] {
  return frame.filter(([iid]) => iid === inboxId).map(([, sub]) => sub);
}

export function matched(
  granted: unknown,
  owned: Subscription[],
): Subscription | null {
  if (granted instanceof Subscription) {
    for (const sub of owned) {
      if (sub === granted) return granted;
    }
  }
  if (owned.length === 1) return owned[0]!;
  return null;
}

export function live(owner: Subscription): Subscription {
  if (typeof owner.active !== "function") {
    throw new Error("lease does not carry its subscription");
  }
  return owner;
}

export function right(
  inboxId: number,
  granted: unknown,
  frame: Array<[number, Subscription]>,
): Subscription {
  const owner = matched(granted, mine(inboxId, frame));
  if (owner === null) throw new Error("lease does not carry its subscription");
  return live(owner);
}

