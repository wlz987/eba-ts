import { Bus, ManualClock, Runtime, SeqIdGen } from "../src/index.ts";

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
runtime.close();
try {
  runtime.advance();
} catch (error) {
  if (error instanceof Error && error.message === "closed or reentrant runtime") {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("advance after close should be refused");
