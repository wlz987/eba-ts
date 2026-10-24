import {
  Bus,
  Inbox,
  Job,
  JobHost,
  ManualClock,
  Runtime,
  SeqIdGen,
} from "../src/index.ts";
import { Caller } from "../src/job/index.ts";
import { Drive } from "../src/runtime/drive.ts";
import type { Envelope } from "../src/envelope/envelope.ts";
import type { Context } from "../src/job/job.ts";

class SilentJob extends Job {
  override begin(_ctx: Context): void {
  }
}

class Silent extends JobHost {
  override makeJob(root: Envelope): Job {
    return new SilentJob(root);
  }
}

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
runtime.add(
  new Silent({
    actorId: "silent",
    receiver: new Inbox(8).receiver(),
    patterns: ["silent.work"],
  }),
);
const caller = new Caller(new Inbox(8).receiver(), runtime.idGenRef);
const drive = new Drive(runtime);
void drive.start();
const pending = drive.open(caller, {
  topic: "silent.work",
  payload: { n: 1 },
  from: "silent",
});
const completion = await pending.ready;
await drive.close();
const body = completion.body as { ok?: boolean; error?: string } | undefined;
if (body?.ok !== false || body.error !== "job_silent") {
  throw new Error(`expected job_silent, got ${JSON.stringify(body)}`);
}
console.log("silent:", body.error);
