import { Bus, Inbox, Job, JobHost, ManualClock, Runtime, SeqIdGen } from "../src/index.ts";
import type { Envelope } from "../src/envelope/envelope.ts";
import type { Context } from "../src/job/job.ts";

class SilentJob extends Job {
  override begin(_ctx: Context): void {}
}

class Silent extends JobHost {
  override makeJob(root: Envelope): Job {
    return new SilentJob(root);
  }
}

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
const a = new Silent({ actorId: "a", receiver: new Inbox(8).receiver(), patterns: [] });
const b = new Silent({ actorId: "b", receiver: new Inbox(8).receiver(), patterns: [] });
runtime.add(a);
runtime.add(b);
runtime.shutdown("a");
if (a.state !== "draining" || b.state !== "running") {
  throw new Error(`selective drain leaked: ${a.state}/${b.state}`);
}
runtime.close();
console.log("drain:", a.state, b.state);
