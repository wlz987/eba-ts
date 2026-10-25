import {
  Bus,
  Inbox,
  Job,
  JobHost,
  ManualClock,
  Runtime,
  SeqIdGen,
} from "../src/index.ts";
import type { Envelope } from "../src/envelope/envelope.ts";
import type { Context } from "../src/job/job.ts";
import { Caller } from "../src/job/index.ts";

let held = false;
let heldSignal: AbortSignal = new AbortController().signal;

class Hold extends Job {
  override begin(ctx: Context): void {
    heldSignal = ctx.hold().signal;
    held = true;
  }
}

class HoldHost extends JobHost {
  override makeJob(root: Envelope): Job {
    return new Hold(root);
  }
}

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
runtime.add(
  new HoldHost({
    actorId: "hold",
    receiver: new Inbox(4).receiver(),
    patterns: ["closed.hold"],
  }),
);
const caller = new Caller(new Inbox(4).receiver(), runtime.idGenRef);
runtime.add(caller);
caller.admit({ topic: "closed.hold", payload: { n: 1 }, from: "closed" });
for (let index = 0; index < 10 && !held; index += 1) runtime.advance();
if (!held) throw new Error("job never held");
if (heldSignal.aborted) throw new Error("signal aborted before close");
runtime.close();
if (!heldSignal.aborted) throw new Error("held signal not aborted on close");

try {
  caller.admit({ topic: "closed.work", payload: { n: 1 }, from: "closed" });
} catch (error) {
  if (error instanceof Error && error.message === "inbox closed") {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("admit after close should be refused");
