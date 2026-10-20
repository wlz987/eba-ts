import type { Core } from "../job/core.ts";
import type { JobHost } from "../job/host.ts";
import { coreOf } from "../job/host.ts";
import { attach, hasWork, scheduled } from "../job/pump.ts";
import { combineFailures } from "../errors.ts";
import { RuntimeBase } from "./base.ts";

export class RuntimeAdmission extends RuntimeBase {
  add(host: JobHost): void {
    this.enter();
    try {
      if (this.state !== "running")
        throw new Error("runtime cannot accept hosts");
      const core = coreOf(host);
      if (core.signal.handed) throw new Error("host already admitted");
      const actor = String(core.sub.actorId);
      if (this.hosts.has(actor)) throw new Error("duplicate host actor");
      attach(core, this.bus);
      this.hosts.set(actor, host);
      core.signal.bind(this.agenda, actor);
      this.sync(core);
      if (this.state !== "running") {
        const refused = new Error("runtime cannot accept hosts");
        this.hosts.delete(actor);
        this.agenda.remove(actor);
        core.signal.detach();
        try {
          core.sub.stop();
        } catch (stuck) {
          throw combineFailures(refused, stuck);
        }
        throw refused;
      }
    } finally {
      this.busy = false;
    }
  }


  protected sync(core: Core): void {
    const closing = core.state === "closing";
    const retired = core.state === "closed";
    this.agenda.update(
      String(core.sub.actorId),
      closing || retired ? null : core.book.nextDeadlineMs,
      closing || (!retired && hasWork(core)),
      !closing && !retired && scheduled(core),
    );
  }


  protected enter(): void {
    if (this.busy || this.readingClock || this.state === "closed") {
      throw new Error("closed or reentrant runtime");
    }
    this.busy = true;
  }

}
