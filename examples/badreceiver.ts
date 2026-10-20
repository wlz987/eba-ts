import { Inbox, Job, JobHost } from "../src/index.ts";
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

const receiver = new Inbox(8).receiver();
new Silent({ actorId: "a", receiver, patterns: [] });
try {
  new Silent({ actorId: "b", receiver, patterns: [] });
} catch (error) {
  if (error instanceof Error && error.message === "receiver transferred or closed") {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("second receiver should be refused");
