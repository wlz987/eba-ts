import type { Bus } from "../mail/bus.ts";
import type { Clock } from "../time/clock.ts";
import type { IdGen } from "../envelope/idgen.ts";
import type { JobHost } from "../job/host.ts";
import { coreOf } from "../job/host.ts";
import { Agenda } from "../notify/agenda.ts";
import { closeCount } from "../mail/inbox.ts";

type RuntimeState = "running" | "draining" | "closing" | "closed";

export class RuntimeBase {
  protected hosts = new Map<string, JobHost>();
  protected agenda = new Agenda();
  protected busy = false;
  protected readingClock = false;
  protected observed: number | null = null;
  protected state: RuntimeState = "running";
  protected noticedCloses = closeCount();
  protected bus: Bus;
  protected clock: Clock;
  protected idGen: IdGen;

  constructor(bus: Bus, clock: Clock, idGen: IdGen) {
    this.bus = bus;
    this.clock = clock;
    this.idGen = idGen;
  }

  get busRef(): Bus {
    return this.bus;
  }
  get clockRef(): Clock {
    return this.clock;
  }
  get idGenRef(): IdGen {
    return this.idGen;
  }

  owns(host: JobHost): boolean {
    const core = coreOf(host);
    return this.hosts.get(String(core.sub.actorId)) === host;
  }

  bindWake(wake: (() => void) | null): void {
    this.agenda.bindWake(wake);
  }
}
