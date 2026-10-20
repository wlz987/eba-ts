import {
  Bus,
  Caller,
  Drive,
  Inbox,
  Job,
  JobHost,
  ManualClock,
  Runtime,
  SeqIdGen,
} from "../src/index.ts";
import type { Envelope } from "../src/envelope/envelope.ts";
import type { Context } from "../src/job/job.ts";

class FailJob extends Job {
  override begin(_ctx: Context): void {
    throw new Error("boom");
  }
}

class Fail extends JobHost {
  override makeJob(root: Envelope): Job {
    return new FailJob(root);
  }
}

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
runtime.add(
  new Fail({
    actorId: "fail",
    receiver: new Inbox(8).receiver(),
    patterns: ["fail.work"],
  }),
);
const caller = new Caller(new Inbox(8).receiver(), runtime.idGenRef);
const drive = new Drive(runtime);
void drive.start();
const pending = drive.open(caller, {
  topic: "fail.work",
  payload: { n: 1 },
  from: "fail",
});
const completion = await pending.ready;
await drive.close();
const body = completion.body as
  | { ok?: boolean; error?: string; message?: string }
  | undefined;
if (body?.ok !== false || body.error !== "job_failed" || !body.message?.includes("boom")) {
  throw new Error(`expected job_failed/boom, got ${JSON.stringify(body)}`);
}
console.log("failed:", body.error, body.message);
