import type { Control, Context, Intent } from "../context.ts";
import { bufferOf } from "../context.ts";

export class Commit {
  readonly requests: Intent[];
  readonly reply: Record<string, unknown> | null;
  readonly control: Control | null;
  constructor(
    requests: Intent[],
    reply: Record<string, unknown> | null,
    control: Control | null,
  ) {
    this.requests = requests;
    this.reply = reply;
    this.control = control;
  }
}

export function sealContext(ctx: Context): Commit {
  const buffer = bufferOf(ctx);
  buffer.idGen = null;
  return new Commit(buffer.requests, buffer.reply, buffer.control);
}
