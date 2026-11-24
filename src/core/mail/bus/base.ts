import { splitTopic } from "#src/core/envelope.js";
import { parsePattern } from "#src/core/pattern.js";
import type { Inbox } from "#src/core/mail/inbox.js";
import { Routes } from "#src/core/mail/routes.js";
import { DeliveryWatch, Subscription } from "#src/core/mail/bus/types.js";
import { DedupWindow } from "#src/core/record.js";

export function busRoutes(bus: BusBase): Routes {
  return bus.$routes;
}

/** The bus ingress dedup window (1024 keys, FIFO eviction). */
export function busDedup(bus: BusBase): DedupWindow {
  return bus.$dedup;
}

export function armDelivery(bus: BusBase, watch: DeliveryWatch): void {
  bus.$deliveries.push(watch);
}

/** Live offers for internal teardown paths (never the public view). */
export function deliveriesOf(bus: BusBase): DeliveryWatch[] {
  return bus.$deliveries;
}

export function retainDeliveries(bus: BusBase, kept: DeliveryWatch[]): void {
  const deliveries = bus.$deliveries;
  deliveries.length = 0;
  deliveries.push(...kept);
}

/** Retire an armed delivery offer; returns false when it was not armed. */
export function retireDelivery(bus: BusBase, watch: DeliveryWatch): boolean {
  const deliveries = bus.$deliveries;
  const index = deliveries.indexOf(watch);
  if (index === -1) return false;
  deliveries.splice(index, 1);
  return true;
}

export function addWait(bus: BusBase, requestId: string): void {
  bus.$waits.add(requestId);
}

export function dropWait(bus: BusBase, requestId: string): void {
  bus.$waits.delete(requestId);
}

export function openGrants(bus: BusBase): Array<[number, Subscription]> {
  const frame: Array<[number, Subscription]> = [];
  bus.$grantFrames.push(frame);
  return frame;
}

export function closeGrants(bus: BusBase): void {
  bus.$grantFrames.pop();
}

export function noteGrant(
  bus: BusBase,
  inbox: Inbox,
  granted: Subscription,
): void {
  const frames = bus.$grantFrames;
  if (frames.length > 0) {
    frames[frames.length - 1]!.push([inbox.id, granted]);
  }
}

export class BusBase {
  readonly $dedup = new DedupWindow(1024);
  $deliveries: DeliveryWatch[] = [];
  $waits = new Set<string>();
  readonly $routes = new Routes();
  $grantFrames: Array<Array<[number, Subscription]>> = [];
  /** The armed delivery offers */

  /** Requests the runtime is waiting on */
  get waits(): ReadonlySet<string> {
    return this.$waits;
  }

  subscribe(pattern: string, inbox: Inbox): Subscription {
    if (inbox.isClosed) throw new Error("inbox closed");
    const parsed = parsePattern(pattern);
    const routes = this.$routes;
    routes.subscribe(parsed, inbox);
    const granted = new Subscription(() => {
      const [found, pending] = routes.detach(parsed, inbox, granted);
      if (pending !== null) routes.changes.notify(pending);
      return found;
    });
    routes.bind(parsed, inbox, granted);
    noteGrant(this, inbox, granted);
    return granted;
  }

  subscriptionCount(topic: string, inbox: Inbox): number {
    let parts: string[];
    try {
      parts = splitTopic(topic);
    } catch {
      return 0;
    }
    return this.$routes.holdCount(parts, inbox);
  }
}
