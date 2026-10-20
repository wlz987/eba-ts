import {
  Bus,
  Caller,
  Inbox,
  MailboxFull,
  ManualClock,
  Runtime,
  SeqIdGen,
} from "../src/index.ts";

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
const caller = new Caller(new Inbox(1).receiver(), runtime.idGenRef);
runtime.add(caller);
caller.admit({ topic: "full.work", payload: { n: 1 }, from: "full" });
try {
  caller.admit({ topic: "full.work", payload: { n: 2 }, from: "full" });
} catch (error) {
  if (error instanceof MailboxFull) {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("second admit should be refused");
