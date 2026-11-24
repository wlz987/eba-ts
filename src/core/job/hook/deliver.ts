import type { Bus } from "#src/core/mail/bus.js";
import type { Envelope } from "#src/core/envelope.js";
import { DeliveryError, combineFailures } from "#src/core/errors.js";
import type { Core } from "#src/core/job/core.js";
import type { State } from "#src/core/job/job.js";
import { Control, Job, markReplied, setPendingControl, stateOf } from "#src/core/job/job.js";
import { holdOrSend, holdsLetter, leaveEnding, settleEnding } from "#src/core/job/execution.js";

export function deliverEnding(
  core: Core,
  bus: Bus,
  job: Job,
  reply: Envelope | null,
  control: Control | null,
): void {
  const state = stateOf(job);
  if (reply !== null) {
    let accepted: boolean;
    try {
      accepted = holdOrSend(core, bus, job, reply);
    } catch (error) {
      if (error instanceof DeliveryError) {
        markReplied(state);
        leaveEnding(job, control);
      }
      throw error;
    }
    markReplied(state);
    if (!accepted) {
      leaveEnding(job, control);
      return;
    }
  }
  settleEnding(core, job, control);
}

export function deferEnding(
  core: Core,
  bus: Bus,
  job: Job,
  state: State,
  control: Control,
  reply: Envelope | null,
  error: Error,
): void {
  setPendingControl(state, control);
  if (reply === null) return;
  try {
    holdOrSend(core, bus, job, reply);
    markReplied(state);
  } catch (deliveryError) {
    if (holdsLetter(core, reply)) {
      markReplied(state);
    }
    throw combineFailures(error, deliveryError);
  }
}

