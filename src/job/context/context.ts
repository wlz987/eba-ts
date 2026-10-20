import type { Envelope } from "../../envelope/envelope.ts";
import type { EnvelopeId } from "../../envelope/ids.ts";
import type { ActorId } from "../../envelope/ids.ts";
import type { IdGen } from "../../envelope/idgen.ts";
import type { Job } from "../job.ts";
import { Buffer } from "./buffer.ts";
import { finishControl, mutate, requestIntent } from "./ops.ts";
import { bufferOf, buffers, putReply, stateOf } from "../context.ts";

export class Context {
  constructor(
    job: Job,
    options: {
      actorId: ActorId;
      replyTo: string;
      idGen: IdGen;
      pendingCount: number;
    },
  ) {
    buffers.set(
      this,
      new Buffer(
        stateOf(job),
        options.actorId,
        options.replyTo,
        options.idGen,
        options.pendingCount,
      ),
    );
  }
  get root(): Envelope {
    return bufferOf(this).job.root;
  }
  get finished(): boolean {
    const buffer = bufferOf(this);
    return buffer.control !== null || buffer.job.lifecycle === "finished";
  }
  request(options: {
    stage: string;
    topic: string;
    payload: unknown;
    timeoutMs?: number | null;
  }): EnvelopeId {
    return mutate(this, (buffer) =>
      requestIntent(
        buffer,
        options.stage,
        options.topic,
        options.payload,
        options.timeoutMs ?? null,
      ),
    );
  }
  reply(body: unknown): void {
    mutate(this, (buffer) => putReply(buffer, body));
  }
  finish(body?: unknown): void {
    mutate(this, (buffer) => finishControl(buffer, body ?? null));
  }
}
