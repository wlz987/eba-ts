import type { IdGen } from "../envelope/idgen.ts";
import { Matchmaker } from "../envelope/reply.ts";
import type { Bus } from "../mail/bus.ts";
import { Control, stateOf } from "./context.ts";
import type { Core } from "./core.ts";
import { seal } from "./ending.ts";
import { deliverEnding } from "./hook/deliver.ts";

/** Settle held replies whose slot has a body. Called at the end of a runtime turn. */
export function resolveHeld(core: Core, bus: Bus, idGen: IdGen): boolean {
  let progressed = false;
  for (const [requestId, reply] of [...core.held]) {
    const body = reply.take();
    if (body === null) continue;
    const job = core.jobs.get(requestId);
    const state = job === undefined ? undefined : stateOf(job);
    if (job === undefined || state === undefined || !state.held) {
      core.held.delete(requestId);
      reply.close();
      continue;
    }
    core.held.delete(requestId);
    state.held = false;
    reply.close();
    const result = new Matchmaker(state.root.header).reply(
      body,
      core.sub.actorId,
      idGen,
    );
    seal(core, job);
    deliverEnding(core, bus, job, result, new Control(body));
    progressed = true;
  }
  return progressed;
}

export function closeHeld(core: Core): void {
  for (const reply of [...core.held.values()]) {
    reply.cancel("host_closing");
    reply.close();
  }
  core.held.clear();
}
