import type { Bus } from "#src/core/mail/bus.js";
import type { Envelope } from "#src/core/envelope.js";
import { DeliveryError, MailboxBusy, combineFailures } from "#src/core/errors.js";
import type { Core } from "#src/core/job/core.js";
import { Job, setPendingControl, stateOf } from "#src/core/job/job.js";
import { settleEnding, waitsForBook } from "#src/core/job/ending.js";
import { Interest } from "#src/core/notify.js";
import { mailboxSpace } from "#src/core/job/mailbox/base.js";
import { owns, park, parkedJob, unpark } from "#src/core/job/delivery/parking.js";
export { holdsLetter, owns } from "#src/core/job/delivery/parking.js";

export function noteDeliveryFailure(core: Core, envelope: Envelope): void {
  if (!owns(core, envelope)) return;
  const key = envelope.header.id;
  const count = (core.attempts.get(key) ?? 0) + 1;
  core.noteAttempt(key, count);
  if (count < 2) {
    core.markAgain(key);
    core.mailbox.rearm(envelope);
    return;
  } else {
    const job = parkedJob(core, envelope);
    core.clearAgain(key);
    unpark(core, envelope);
    core.mailbox.bury(envelope);
    // The attempt *history* survives the bury: `attempts` answers "failed N
    // times", `again` answers "due for one more try", and only the second is a
    // schedule. Clearing both made `spent()` (`attempts.has && !again.has`)
    // false for every envelope, so the rescue path for an undeliverable answer
    // was unreachable. `noteDeliverySuccess`/`releaseAttempts` clear the history.
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
  setPendingControl(state, null);
  if (control !== null) settleEnding(core, job, control);
}

export function holdOrSend(
  core: Core,
  bus: Bus,
  job: Job | null,
  envelope: Envelope,
): boolean {
  try {
    core.mailbox.send(bus, envelope);
    return true;
  } catch (error) {
    if (!(error instanceof DeliveryError)) throw error;
    const full = error instanceof MailboxBusy;
    park(core, job, envelope);
    if (full) {
      if (core.mailboxWait === null) {
        const wait = new Interest(core.signal, {}, "mailbox");
        core.armMailboxWait(wait);
        mailboxSpace(core.mailbox).watch(wait);
      }
      return false;
    }
    raiseDeliveryFailure(core, error);
  }
}
export { nextSendable, releaseResult, retryDue } from "#src/core/job/delivery/release.js";

export function releaseAttempts(core: Core): void {
  core.releaseAttempts();
}

export function noteDeliverySuccess(core: Core, envelope: Envelope): void {
  const key = envelope.header.id;
  core.clearAttempt(key);
  core.clearAgain(key);
}
