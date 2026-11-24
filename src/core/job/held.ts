import type { IdGen } from "#src/core/envelope.js";
import type { Bus } from "#src/core/mail/bus.js";
import { Control, stateOf } from "#src/core/job/context.js";
import { markHeld } from "#src/core/job/job.js";
import type { Core } from "#src/core/job/core.js";
import { seal } from "#src/core/job/ending.js";
import { deliverEnding } from "#src/core/job/hook/deliver.js";
import { replyFor } from "#src/core/job/execution.js";

export function resolveHeld(core: Core, bus: Bus, idGen: IdGen): boolean {
  // The copy is needed because the loop *mutates* `held` (`releaseHeld`), so a
  // live iterator would be invalidated. But the copy is not free: `[...map]`
  // allocates the array plus one tuple per entry even when the map is empty —
  // measured **43ns** for `[...emptyMap]` vs **3.7ns** for the `size === 0`
  // guard (12×). `resolveHeld` runs once per host per turn, and a host's `held`
  // book is empty on almost every turn (it only fills when a job holds a reply
  // slot), so this guard is on the hot path. Same reasoning as
  // `Core.takePendingCancels`'s empty-set short-circuit.
  if (core.held.size === 0) return false;
  let progressed = false;
  for (const [requestId, reply] of [...core.held]) {
    const body = reply.take();
    if (body === null) continue;
    const job = core.jobs.get(requestId);
    const state = job === undefined ? undefined : stateOf(job);
    if (job === undefined || state === undefined || !state.held) {
      core.releaseHeld(requestId);
      reply.close();
      continue;
    }
    core.releaseHeld(requestId);
    markHeld(state, false);
    reply.close();
    const result = replyFor(state.root, body, core, idGen);
    seal(core, job);
    deliverEnding(core, bus, job, result, new Control(body));
    progressed = true;
  }
  return progressed;
}

export function closeHeld(core: Core): void {
  // Same empty-book guard as `resolveHeld` above: `closeHeld` runs on every host
  // retirement (including the routine `Runtime.close()` of a host that never
  // held anything), and `[...map.values()]` costs 43ns even when empty.
  if (core.held.size === 0) return;
  // Closing walks the jobs: each held job keeps its own `state.held`
  // bit, which must be cleared or `cancelJob` refuses it forever. Walking
  // `core.jobs` also covers the window where the lease was already retracted
  // but the job's bit is still set.
  for (const job of core.jobs.values()) {
    markHeld(stateOf(job), false);
  }
  for (const reply of [...core.held.values()]) {
    reply.revoke("host_closing");
    reply.close();
  }
  core.clearHeld();
}
