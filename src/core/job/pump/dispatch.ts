import type { Bus } from "#src/core/mail/bus.js";
import { Envelope, snapshotEnvelope, EnvelopeBuildError } from "#src/core/envelope.js";
import type { IdGen } from "#src/core/envelope.js";
import type { Core } from "#src/core/job/core.js";
import { keepUnread, reject, refused } from "#src/core/job/execution.js";

export function dispatch(
  core: Core,
  bus: Bus,
  idGen: IdGen,
  envelope: Envelope,
): void {
  const frozen = snapshotOrKeep(core, envelope);
  if (frozen === null) return;
  if (frozen.header.address.kind === "diagnostic") return;
  let reason = refused(core);
  if (reason === null && !core.queuePush(frozen)) reason = "queue_full";
  if (reason !== null) reject(core, bus, idGen, frozen, reason);
}

export function snapshotOrKeep(core: Core, envelope: Envelope): Envelope | null {
  try {
    return snapshotEnvelope(envelope);
  } catch (error) {
    if (error instanceof EnvelopeBuildError || error instanceof TypeError) {
      keepUnread(core, envelope);
      return null;
    }
    throw error;
  }
}

