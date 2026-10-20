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
runtime.add(new Silent({ actorId: "same", receiver: new Inbox(8).receiver(), patterns: [] }));
try {
  runtime.add(new Silent({ actorId: "same", receiver: new Inbox(8).receiver(), patterns: [] }));
} catch (error) {
  if (error instanceof Error && error.message === "duplicate host actor") {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("duplicate host should be refused");
