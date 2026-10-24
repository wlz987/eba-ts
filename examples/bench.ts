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
import { Caller } from "../src/job/index.ts";
import { Drive } from "../src/runtime/drive.ts";
import type { Envelope } from "../src/envelope/envelope.ts";
import type { Context } from "../src/job/job.ts";

class EchoJob extends Job {
  override begin(ctx: Context): void {
    const n = (ctx.root.payload as Record<string, unknown>)["n"];
    ctx.finish(okPayload({ n }));
  }
}

class EchoHost extends JobHost {
  override makeJob(root: Envelope): Job {
    return new EchoJob(root);
  }
}

async function main(): Promise<void> {
  const bus = new Bus();
  const runtime = new Runtime(bus, new ManualClock(), new SeqIdGen());
  runtime.add(
    new EchoHost({
      actorId: "echo",
      receiver: new Inbox(64).receiver(),
      patterns: ["echo.work"],
    }),
  );
  const caller = new Caller(new Inbox(64).receiver(), runtime.idGenRef);
  const drive = new Drive(runtime);
  void drive.start();
  for (let i = 0; i < 50; i += 1) {
    await drive.open(caller, {
      topic: "echo.work",
      payload: { n: 0 },
      from: "bench",
    }).ready;
  }
  const total = 5000;
  const window = 64;
  const started = performance.now();
  let done = 0;
  while (done < total) {
    const batch = Math.min(window, total - done);
    const pending = [];
    for (let i = 0; i < batch; i += 1) {
      pending.push(
        drive.open(caller, {
          topic: "echo.work",
          payload: { n: done + i },
          from: "bench",
        }),
      );
    }
    await Promise.all(pending.map((item) => item.ready));
    done += batch;
  }
  const elapsed = (performance.now() - started) / 1000;
  await drive.close();
  console.log(
    `${total} messages in ${elapsed.toFixed(3)}s = ${Math.round(total / elapsed)} msg/s`,
  );
}

await main();
