import type { Bus } from "../mail/bus.ts";
import type { Envelope } from "../envelope/envelope.ts";
import { DeliveryError, OutboxFull, combineFailures } from "../errors.ts";
import type { Core } from "./core.ts";
import { Job, stateOf } from "./job.ts";
import { settleEnding, waitsForBook } from "./ending.ts";
export { noteDeliverySuccess, releaseAttempts } from "./delivery/notes.ts";
import { owns, park, parkedJob, unpark } from "./delivery/parking.ts";
export { holdsLetter, owns } from "./delivery/parking.ts";

export function noteDeliveryFailure(core: Core, envelope: Envelope): void {
  if (!owns(core, envelope)) return;
  const count = (core.attempts.get(envelope) ?? 0) + 1;
  core.attempts.set(envelope, count);
  if (count < 2) {
    core.again.add(envelope);
    core.outbox.rearm(envelope);
    return;
  } else {
    const job = parkedJob(core, envelope);
    core.again.delete(envelope);
    unpark(core, envelope);
    core.outbox.bury(envelope);
    completeHeld(core, job);
  }
}

export function raiseDeliveryFailure(core: Core, error: DeliveryError): never {
  try {
    noteDeliveryFailure(core, error.envelope);
  } catch (secondary) {
    throw combineFailures(error, secondary);
  }
  throw error;
}

export function completeHeld(core: Core, job: Job | null): void {
  if (job === null) return;
  const state = stateOf(job);
  if (waitsForBook(core, state)) return;
  const control = state.pendingControl;
  state.pendingControl = null;
  if (control !== null) settleEnding(core, job, control);
}

export function holdOrSend(
  core: Core,
  bus: Bus,
  job: Job | null,
  envelope: Envelope,
): boolean {
  try {
    core.outbox.send(bus, envelope);
    return true;
  } catch (error) {
    if (!(error instanceof DeliveryError)) throw error;
    const full = error instanceof OutboxFull;
    park(core, job, envelope);
    if (full) return false;
    raiseDeliveryFailure(core, error);
  }
}
export { nextSendable, releaseResult, retryDue } from "./delivery/release.ts";
