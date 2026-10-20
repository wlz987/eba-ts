import type { Envelope } from "../../envelope/envelope.ts";
import type { Bus } from "../../mail/bus.ts";
import type { Core } from "../core.ts";
import type { Job } from "../job.ts";
import { DeliveryError, OutboxFull } from "../../errors.ts";
import { completeHeld, noteDeliverySuccess, raiseDeliveryFailure } from "../delivery.ts";

function spent(core: Core, envelope: Envelope): boolean {
  return core.attempts.has(envelope) && !core.again.has(envelope);
}

export function nextSendable(
  core: Core,
): ["pending", number] | ["rest", number] | null {
  if (core.pending !== null && !spent(core, core.pending))
    return ["pending", -1];
  for (let index = 0; index < core.rest.length; index += 1) {
    if (!spent(core, core.rest[index]![0])) return ["rest", index];
  }
  return null;
}

export function retryDue(core: Core): boolean {
  if (!core.outbox.hasRoom()) return false;
  return nextSendable(core) !== null;
}

export function releaseResult(core: Core, bus: Bus): boolean {
  let sent = false;
  while (true) {
    const found = nextSendable(core);
    if (found === null || !core.outbox.hasRoom()) return sent;
    let envelope: Envelope;
    let job: Job | null;
    let kind: "pending" | "rest";
    let index = -1;
    if (found[0] === "pending") {
      kind = "pending";
      envelope = core.pending!;
      job = core.pendingJob;
      core.pending = null;
      core.pendingJob = null;
    } else {
      kind = "rest";
      index = found[1];
      [envelope, job] = core.rest[index]!;
      core.rest.splice(index, 1);
    }
    try {
      core.outbox.send(bus, envelope);
    } catch (error) {
      if (error instanceof DeliveryError) {
        if (kind === "pending" && core.pending === null) {
          core.pending = envelope;
          core.pendingJob = job;
        } else {
          core.rest.splice(kind === "pending" ? 0 : index, 0, [envelope, job]);
        }
        if (error instanceof OutboxFull) return sent;
        raiseDeliveryFailure(core, error);
      }
      throw error;
    }
    if (!core.outbox.contains(envelope)) noteDeliverySuccess(core, envelope);
    completeHeld(core, job);
    sent = true;
  }
}

