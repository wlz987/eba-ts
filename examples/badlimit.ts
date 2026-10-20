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

try {
  new Silent({
    actorId: "x",
    receiver: new Inbox(8).receiver(),
    patterns: [],
    activeLimit: 0,
  });
} catch (error) {
  if (error instanceof Error && error.message === "active_limit must be a positive integer") {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("zero active limit should be refused");
