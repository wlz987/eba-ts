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
const first = runtime.close("a");
if (first.length !== 0 || a.state !== "closed" || b.state !== "running") {
  throw new Error(`selective close leaked: ${first.length}/${a.state}/${b.state}`);
}
const second = runtime.close();
const bState: string = b.state;
if (second.length !== 0 || bState !== "closed") {
  throw new Error(`rest close leaked: ${second.length}/${bState}`);
}
console.log("selective:", first.length, second.length);
