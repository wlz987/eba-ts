import { Bus, Inbox, Job, JobHost, ManualClock, Runtime, SeqIdGen, okPayload } from "../src/index.ts";
import type { Context } from "../src/job/job.ts";
import type { Envelope } from "../src/envelope/envelope.ts";
import { Caller } from "../src/job/index.ts";

let seed = 7;
const random = (): number => {
  seed = (seed * 1664525 + 1013904223) >>> 0;
  return seed / 4294967296;
};

class Echo extends Job {
  override begin(ctx: Context): void {
    ctx.reply(okPayload({ n: 1 }));
  }
}

class Hold extends Job {
  override begin(ctx: Context): void {
    ctx.hold();
  }
}

class Bad extends Job {
  override begin(ctx: Context): void {
    ctx.request({ stage: "inner", topic: "hold", payload: {} });
  }

  override onFinished(_body: Record<string, unknown> | null): void {
    throw new Error("cleanup");
  }
}

class EchoHost extends JobHost {
  override makeJob(root: Envelope): Job {
    return new Echo(root);
  }
}

class HoldHost extends JobHost {
  override makeJob(root: Envelope): Job {
    return new Hold(root);
  }
}

class BadHost extends JobHost {
  override makeJob(root: Envelope): Job {
    return new Bad(root);
  }
}

let cancelErrors = 0;
for (let wave = 0; wave < 100; wave += 1) {
  const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
  runtime.add(new EchoHost({ actorId: "echo", receiver: new Inbox(64).receiver(), patterns: ["echo"] }));
  runtime.add(new HoldHost({ actorId: "hold", receiver: new Inbox(64).receiver(), patterns: ["hold"] }));
  runtime.add(new BadHost({ actorId: "bad", receiver: new Inbox(64).receiver(), patterns: ["bad"] }));
  const caller = new Caller(new Inbox(256).receiver(), runtime.idGenRef);
  runtime.add(caller);
  const roots: Array<{ topic: string; root: Envelope }> = [];
  for (let index = 0; index < 20; index += 1) {
    const pick = Math.floor(random() * 3);
    const topic = pick === 0 ? "echo" : pick === 1 ? "hold" : "bad";
    roots.push({ topic, root: caller.admit({ topic, payload: { n: index }, from: "soak" }) });
  }
  for (let index = 0; index < 3; index += 1) runtime.advance();
  for (const { topic, root } of roots) {
    if (topic === "echo") continue;
    if (random() < 0.25) {
      (root.payload as { pay: ((completion: unknown) => void) | null }).pay = () => {
        throw new Error("pay");
      };
    }
    try {
      caller.cancel(root.header.id);
    } catch {
      // user pay may throw after the job is dropped; leak is still checked below
    }
  }
  for (let index = 0; index < 12; index += 1) runtime.advance();
  const stats = runtime.stats();
  if (stats.active || stats.held || stats.pending || stats.queued) {
    throw new Error(`soak leak wave ${wave}: ${JSON.stringify(stats)}`);
  }
  for (const detail of Object.values(stats.hostsDetail)) {
    if (detail.active || detail.held || detail.pending || detail.queued) {
      throw new Error(`soak host leak wave ${wave}: ${JSON.stringify(detail)}`);
    }
  }
  cancelErrors += stats.cancelErrors;
  runtime.close();
}
if (cancelErrors === 0) throw new Error("soak did not exercise cancelErrors");
console.log(`soak ok: 100 waves x 20 asks, ${cancelErrors} contained hook errors`);
