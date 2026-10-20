import { Bus, ManualClock, Runtime, SeqIdGen } from "../src/index.ts";

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
const first = runtime.close();
const second = runtime.close();
if (first.length !== 0 || second.length !== 0) {
  throw new Error(`expected two empty closes, got ${first.length}/${second.length}`);
}
console.log("reclose:", first.length, second.length);
