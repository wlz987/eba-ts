import { Bus, Inbox, ManualClock, Runtime, SeqIdGen } from "../src/index.ts";
import { Caller } from "../src/job/index.ts";

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
const caller = new Caller(new Inbox(4).receiver(), runtime.idGenRef);
runtime.add(caller);
runtime.close();
try {
  caller.admit({ topic: "closed.work", payload: { n: 1 }, from: "closed" });
} catch (error) {
  if (error instanceof Error && error.message === "inbox closed") {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("admit after close should be refused");
