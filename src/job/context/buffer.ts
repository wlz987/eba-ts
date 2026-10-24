import type { ActorId } from "../../envelope/ids.ts";
import type { IdGen } from "../../envelope/idgen.ts";
import type { Core } from "../core.ts";
import type { State } from "../job.ts";
import type { Reply } from "../reply.ts";
import type { Control, Intent } from "../context.ts";

export class Buffer {
  readonly job: State;
  readonly core: Core;
  readonly actorId: ActorId;
  readonly replyTo: string;
  idGen: IdGen | null;
  readonly pendingCount: number;
  busy = false;
  requests: Intent[] = [];
  reply: Record<string, unknown> | null = null;
  control: Control | null = null;
  hold: Reply | null = null;
  constructor(
    job: State,
    actorId: ActorId,
    replyTo: string,
    idGen: IdGen | null,
    pendingCount: number,
    core: Core,
  ) {
    this.job = job;
    this.core = core;
    this.actorId = actorId;
    this.replyTo = replyTo;
    this.idGen = idGen;
    this.pendingCount = pendingCount;
  }
}

