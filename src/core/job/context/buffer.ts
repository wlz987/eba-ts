import type { ActorId, IdGen } from "#src/core/envelope.js";
import type { Core } from "#src/core/job/core.js";
import type { State } from "#src/core/job/job.js";
import type { Lease } from "#src/core/job/lease.js";
import type { Control, Intent } from "#src/core/job/context.js";
import { mintOf } from "#src/core/job/context.js";
import type { EffectHost } from "#src/core/job/effect.js";

export class Buffer {
  readonly job: State;
  readonly core: Core;
  readonly actorId: ActorId;
  readonly replyTo: string;
  readonly pendingCount: number;
  $idGen: IdGen | null;
  $busy = false;
  $requests: Intent[] = [];
  $reply: Record<string, unknown> | null = null;
  $control: Control | null = null;
  $hold: Lease | null = null;
  host: EffectHost | null;
  /** Unforgeable token handed to every intent this buffer mints. */
  readonly $mint: object;

  constructor(
    job: State,
    actorId: ActorId,
    replyTo: string,
    idGen: IdGen | null,
    pendingCount: number,
    core: Core,
    host: EffectHost | null = null,
    mint: object = {},
  ) {
    this.job = job;
    this.core = core;
    this.actorId = actorId;
    this.replyTo = replyTo;
    this.$idGen = idGen;
    this.pendingCount = pendingCount;
    this.host = host;
    this.$mint = mint;
  }

  /** The id generator, or `null` once the context is sealed. */
  get idGen(): IdGen | null {
    return this.$idGen;
  }

  get busy(): boolean {
    return this.$busy;
  }

  /** The asks this hook has declared */
  get requests(): ReadonlyArray<Intent> {
    return this.$requests;
  }

  get reply(): Record<string, unknown> | null {
    return this.$reply;
  }

  get control(): Control | null {
    return this.$control;
  }

  get hold(): Lease | null {
    return this.$hold;
  }

  }

export function setReply(buffer: Buffer, body: Record<string, unknown> | null): void {
  buffer.$reply = body;
}

export function setControl(buffer: Buffer, control: Control | null): void {
  buffer.$control = control;
}

export function setHold(buffer: Buffer, lease: Lease | null): void {
  buffer.$hold = lease;
}

export function pushRequest(buffer: Buffer, intent: Intent): void {
  // The intent must be *this* buffer's; a forged or foreign one is rejected.
  if (mintOf(intent) !== (buffer.$mint)) {
    throw new Error("intent was not minted by this context");
  }
  buffer.$requests.push(intent);
}

export function bufferMint(buffer: Buffer): object {
  return buffer.$mint;
}

/** Make the context unusable (`sealContext` calls this once per hook). */
export function invalidate(buffer: Buffer): void {
  buffer.$idGen = null;
}

/** Enter a `mutate` callback: sets the reentrancy guard. */
export function beginMutation(buffer: Buffer): void {
  buffer.$busy = true;
}

/** Leave a `mutate` callback: clears the reentrancy guard. */
export function endMutation(buffer: Buffer): void {
  buffer.$busy = false;
}
