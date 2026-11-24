import type { Envelope, EnvelopeId, ActorId, IdGen } from "#src/core/envelope.js";
import type { Core } from "#src/core/job/core.js";
import type { Job } from "#src/core/job/job.js";
import type { Lease } from "#src/core/job/lease.js";
import { Buffer } from "#src/core/job/context/buffer.js";
import { buildIntent, mutate, prepareHold } from "#src/core/job/context/ops.js";

import { stateOf } from "#src/core/job/context.js";

import type { AskEffect, BarrierEffect, CloseEffect, Effect, EffectHost, FinishEffect, RecordEffect, ReplyEffect, TellEffect, TimerEffect } from "#src/core/job/effect.js";

function ownedBody(body: unknown): unknown {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return body;
  return { ...(body as Record<string, unknown>) };
}

/** Freeze an effect the builders hand to a handler. */
function freezeEffect<T extends Effect>(effect: T): T {
  return effect;
}

export class Context {
  /** Per-hook scratch buffer, handed to sibling accessors by `bufferOf`. */
  readonly $buffer: Buffer;

  constructor(
    job: Job,
    options: {
      actorId: ActorId;
      replyTo: string;
      idGen: IdGen;
      pendingCount: number;
      core: Core;
      host?: EffectHost;
      mint?: object;
    },
  ) {
    this.$buffer = new Buffer(
      stateOf(job),
      options.actorId,
      options.replyTo,
      options.idGen,
      options.pendingCount,
      options.core,
      options.host ?? null,
      options.mint ?? {},
    );
  }

  get root(): Envelope {
    return this.$buffer.job.root;
  }
  get finished(): boolean {
    return this.effects.some((effect) => effect.kind === "finish")
      || this.$buffer.control !== null
      || this.$buffer.job.lifecycle === "finished";
  }

  /** Effects declared in this turn; builders, hooks and the runtime append here. */
  effects: Effect[] = [];

  pushEffect(effect: Effect): void {
    this.effects.push(effect);
  }

  pushEffects(effects: readonly Effect[]): void {
    this.effects.push(...effects);
  }

  /**
   * Claim the declared effects as a private array.
   *
   * The runtime splits them at the `barrier`/`timer` marker, so it needs an
   * array it owns; handing out the live one would let a later `pushEffect` land
   * in a batch that is already being applied.
   */
  takeEffects(): Effect[] {
    return this.effects.slice();
  }

  request(options: {
    stage: string;
    topic: string;
    payload: unknown;
    timeoutMs?: number | null;
  }): EnvelopeId {
    const effect = this.ask(options);
    this.pushEffect(effect);
    return effect.intent.request.header.id;
  }
  reply(body: unknown): void {
    this.pushEffect(this.replyEffect(body));
  }
  finish(body?: unknown): void {
    this.pushEffect(this.finishEffect(body));
  }
  ask(options: {
    stage: string;
    topic: string;
    payload: unknown;
    timeoutMs?: number | null;
  }): AskEffect {
    const asks = this.effects.filter((effect) => effect.kind === "ask").length;
    return freezeEffect({
      kind: "ask",
      intent: mutate(this, (buffer) =>
        buildIntent(buffer, options.stage, options.topic, options.payload, options.timeoutMs ?? null, asks),
      ),
    });
  }
  replyEffect(body: unknown): ReplyEffect {
    return freezeEffect({ kind: "reply", body: ownedBody(body) });
  }
  finishEffect(body?: unknown): FinishEffect {
    return freezeEffect({ kind: "finish", body: ownedBody(body) });
  }
  tell(topic: string, payload: unknown): TellEffect {
    return freezeEffect({ kind: "tell", topic, payload });
  }
  record(topic: string, payload: unknown): RecordEffect {
    return freezeEffect({ kind: "record", topic, payload });
  }
  barrier(upTo?: number): BarrierEffect {
    return freezeEffect(
      upTo === undefined
        ? { kind: "barrier" }
        : { kind: "barrier", upTo: upTo },
    );
  }
  timer(deadlineMs: number): TimerEffect {
    // Pure builder: staging is the hook's choice (return the effect or push it).
    // The deadline itself is checked when the effect lands, in `Core.waitTimer`.
    return freezeEffect({ kind: "timer", deadlineMs });
  }
  close(reason?: string): CloseEffect {
    return freezeEffect(reason === undefined ? { kind: "close" } : { kind: "close", reason });
  }
  hold(options: import("#src/core/job/lease.js").LeaseOptions = {}): Lease {
    if (this.effects.length > 0) throw new Error("cannot hold after other effects");
    const lease = mutate(this, (buffer) => prepareHold(buffer, options));
    this.pushEffect(freezeEffect({ kind: "hold", lease }));
    return lease;
  }
}

/**
 * Hand a hook's `$buffer` to the sibling modules that act on it.
 *
 * returns the live `Buffer` because `effect`/`ops`/`commit` mutate it, but the
 * class no longer hands out the symbol that reaches it.
 */
export function readBuffer(value: Context): Buffer {
  return value.$buffer;
}
