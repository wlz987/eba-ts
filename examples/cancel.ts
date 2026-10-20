import { Bus, Caller, Drive, Inbox, ManualClock, Runtime, SeqIdGen } from "../src/index.ts";

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
const caller = new Caller(new Inbox(8).receiver(), runtime.idGenRef);
const drive = new Drive(runtime);
await drive.start();
const pending = drive.open(caller, {
  topic: "cancel.work",
  payload: { n: 1 },
  from: "cancel",
});
const cancelled = caller.cancel(pending.envelope.header.id);
const completion = await pending.ready;
await drive.close();
if (!cancelled || completion.body === undefined) {
  throw new Error("cancel should settle with a cancelled body");
}
const body = completion.body as { ok?: boolean; error?: string };
if (body.ok !== false || body.error !== "cancelled") {
  throw new Error(`expected cancelled, got ${JSON.stringify(body)}`);
}
console.log("cancelled:", cancelled, body.error);
