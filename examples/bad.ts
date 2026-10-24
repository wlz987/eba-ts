import {
  Bus,
  EnvelopeBuildError,
  Inbox,
  ManualClock,
  Runtime,
  SeqIdGen,
} from "../src/index.ts";
import { Caller } from "../src/job/index.ts";

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
const caller = new Caller(new Inbox(4).receiver(), runtime.idGenRef);
runtime.add(caller);
try {
  caller.admit({ topic: "bad!", payload: { n: 1 }, from: "bad" });
} catch (error) {
  if (error instanceof EnvelopeBuildError) {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("bad topic should be refused");
