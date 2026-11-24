import type { Envelope } from "#src/core/envelope.js";
import type { Bus } from "#src/core/mail/bus.js";
import type { Core } from "#src/core/job/core.js";
import type { Job } from "#src/core/job/job.js";
import { DeliveryError, MailboxBusy } from "#src/core/errors.js";
import { completeHeld, noteDeliverySuccess, raiseDeliveryFailure } from "#src/core/job/delivery.js";

function spent(core: Core, envelope: Envelope): boolean {
  const key = envelope.header.id;
  return core.attempts.has(key) && !core.again.has(key);
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
  if (!core.mailbox.hasRoom()) return false;
  return nextSendable(core) !== null;
}

export function releaseResult(core: Core, bus: Bus): boolean {
  if (core.mailbox.hasRoom()) core.takeMailboxWait()?.dispose();
  let sent = false;
  while (true) {
    const found = nextSendable(core);
    if (found === null || !core.mailbox.hasRoom()) return sent;
    let envelope: Envelope;
    let job: Job | null;
    let kind: "pending" | "rest";
    let index = -1;
    if (found[0] === "pending") {
      kind = "pending";
      [envelope, job] = core.takeParked()!;
    } else {
      kind = "rest";
      index = found[1];
      [envelope, job] = core.takeRest(index);
    }
    try {
      core.mailbox.send(bus, envelope);
    } catch (error) {
      if (error instanceof DeliveryError) {
        if (kind === "pending" && core.pending === null) {
          core.restoreParked(envelope, job);
        } else {
          core.insertRest(kind === "pending" ? 0 : index, envelope, job);
        }
        if (error instanceof MailboxBusy) return sent;
        raiseDeliveryFailure(core, error);
      }
      throw error;
    }
    if (!core.mailbox.contains(envelope)) noteDeliverySuccess(core, envelope);
    completeHeld(core, job);
    sent = true;
  }
}

