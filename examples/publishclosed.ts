import { Bus, Inbox, makeEnvelope } from "../src/index.ts";

const bus = new Bus();
const inbox = new Inbox(8);
const receiver = inbox.receiver();
bus.subscribe("a.b", inbox);
receiver.close();
const envelope = makeEnvelope({ topic: "a.b", payload: 1, from: "c", id: "e1" });
const delivered = bus.publish(envelope);
if (delivered !== false) {
  throw new Error(`publishclosed leaked: ${delivered}`);
}
console.log("publishclosed:", delivered);
