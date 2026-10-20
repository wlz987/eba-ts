import type { ActorId } from "../../envelope/ids.ts";
import type { IdGen } from "../../envelope/idgen.ts";
import type { State } from "../job.ts";
import type { Control, Intent } from "../context.ts";

export class Buffer {
  readonly job: State;
  readonly actorId: ActorId;
  readonly replyTo: string;
  idGen: IdGen | null;
  readonly pendingCount: number;
  busy = false;
  requests: Intent[] = [];
  reply: Record<string, unknown> | null = null;
  control: Control | null = null;
  constructor(
    job: State,
    actorId: ActorId,
    replyTo: string,
    idGen: IdGen | null,
    pendingCount: number,
  ) {
    this.job = job;
    this.actorId = actorId;
    this.replyTo = replyTo;
    this.idGen = idGen;
    this.pendingCount = pendingCount;
  }
}

