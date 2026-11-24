import type { Bus } from "#src/core/mail/bus.js";
import type { Envelope, IdGen } from "#src/core/envelope.js";
import { combineFailures } from "#src/core/errors.js";
import type { Core } from "#src/core/job/core.js";
import { Matchmaker } from "#src/core/envelope.js";
import { holdOrSend } from "#src/core/job/delivery.js";

/** Build the result envelope that answers a request. */
export function replyFor(root: Envelope, body: unknown, core: Core, idGen: IdGen): Envelope {
  return new Matchmaker(root.header).reply(body, core.sub.actorId, idGen);
}

export function invoke(callback: () => unknown): unknown {
  return callback();
}

export function respond(
  core: Core,
  bus: Bus,
  idGen: IdGen,
  root: Envelope,
  body: unknown,
): void {
  holdOrSend(core, bus, null, replyFor(root, body, core, idGen));
}

export function observe(action: () => void, observer: () => void): void {
  try {
    action();
  } catch (error) {
    try {
      observer();
    } catch (secondary) {
      throw combineFailures(error, secondary);
    }
    throw error;
  }
  observer();
}

export { deadline, reject, silence } from "#src/core/job/execution/lifecycle.js";
export {
  holdsLetter, holdOrSend, nextSendable, noteDeliveryFailure, noteDeliverySuccess, owns,
  raiseDeliveryFailure, releaseAttempts, releaseResult, retryDue,
} from "#src/core/job/delivery.js";
export {
  leaveEnding, refused, releaseDeferred, releaseEnding, remove, seal, settleEnding,
  waitsForBook, waitsForLease,
} from "#src/core/job/ending.js";
export { runHook, deliverEnding } from "#src/core/job/hook.js";
export { adopt, keepUnread, resume } from "#src/core/job/resume.js";
