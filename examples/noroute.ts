import { Bus, makeEnvelope } from "../src/index.ts";

const envelope = makeEnvelope({ topic: "x.y", payload: 1, from: "c", id: "e1" });
const delivered = new Bus().publish(envelope);
if (delivered !== false) {
  throw new Error(`noroute leaked: ${delivered}`);
}
console.log("noroute:", delivered);
