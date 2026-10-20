import type { Bus } from "../../mail/bus.ts";
import { Envelope, snapshotEnvelope } from "../../envelope/envelope.ts";
import type { IdGen } from "../../envelope/idgen.ts";
import { EnvelopeBuildError } from "../../envelope/errors.ts";
import type { Core } from "../core.ts";
import { keepUnread, reject, refused } from "../execution.ts";

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
  if (reason === null && !core.queue.push(frozen)) reason = "queue_full";
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

