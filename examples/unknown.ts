import { Bus, ManualClock, Runtime, SeqIdGen } from "../src/index.ts";

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
try {
  runtime.close("nobody");
} catch (error) {
  if (error instanceof Error && error.message.startsWith("unknown host:")) {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("unknown host should be refused");
