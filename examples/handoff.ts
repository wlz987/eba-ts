import {
  Bus,
  Inbox,
  Job,
  JobHost,
  ManualClock,
  Runtime,
  SeqIdGen,
  okPayload,
} from "../src/index.ts";
import type { Completion } from "../src/index.ts";
import type { Envelope } from "../src/envelope/envelope.ts";
import type { Context } from "../src/job/job.ts";

class Echo extends Job {
  override begin(ctx: Context): void {
    ctx.reply(okPayload((ctx.root.payload as { value: number }).value));
  }
}

class EchoHost extends JobHost {
  override makeJob(root: Envelope): Job {
    return new Echo(root);
  }
}

class Hold extends Job {
  override begin(ctx: Context): void {
    ctx.hold();
  }
}

class HoldHost extends JobHost {
  override makeJob(root: Envelope): Job {
    return new Hold(root);
  }
}

function main(): void {
  const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
  runtime.add(
    new EchoHost({
      actorId: "echo",
      receiver: new Inbox(32).receiver(),
      patterns: ["echo"],
    }),
  );
  runtime.add(
    new HoldHost({
      actorId: "hold",
      receiver: new Inbox(8).receiver(),
      patterns: ["hold"],
    }),
  );
  const seen: Completion[] = [];
  runtime.askWith(
    "echo",
    { value: 7 },
    (completion) => seen.push(completion),
    "handoff",
  );
  for (let index = 0; index < 12; index += 1) runtime.advance();
  const body = seen[0]?.body as { ok?: boolean; value?: number } | undefined;
  if (seen.length !== 1 || body?.ok !== true || body.value !== 7) {
    throw new Error(`handoff: ${JSON.stringify(seen)}`);
  }

  const batch: Completion[] = [];
  for (let value = 0; value < 9; value += 1) {
    runtime.askWith(
      "echo",
      { value },
      (completion) => batch.push(completion),
      "handoff",
    );
  }
  for (let index = 0; index < 24; index += 1) runtime.advance();
  const values = batch
    .map((completion) => (completion.body as { value?: number } | null)?.value ?? -1)
    .sort((left, right) => left - right);
  if (batch.length !== 9 || values.some((value, index) => value !== index)) {
    throw new Error(`handoff batch: ${JSON.stringify(batch)}`);
  }

  const closed: Completion[] = [];
  runtime.askWith(
    "hold",
    { n: 0 },
    (completion) => closed.push(completion),
    "handoff",
  );
  for (let index = 0; index < 4; index += 1) runtime.advance();
  runtime.close();
  const closeBody = closed[0]?.body as { error?: string } | undefined;
  if (closed.length !== 1 || closeBody?.error !== "host_draining") {
    throw new Error(`handoff close: ${JSON.stringify(closed)}`);
  }
  console.log("handoff ok");
}

main();
