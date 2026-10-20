import { BackpressureError, Bus, Caller, Inbox, ManualClock, Runtime, SeqIdGen } from "../src/index.ts";

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
const caller = new Caller(new Inbox(8).receiver(), runtime.idGenRef, 1);
runtime.add(caller);
caller.admit({ topic: "bp.work", payload: { n: 1 }, from: "bp" });
try {
  caller.admit({ topic: "bp.work", payload: { n: 2 }, from: "bp" });
} catch (error) {
  if (error instanceof BackpressureError) {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("second admit should be backpressured");
