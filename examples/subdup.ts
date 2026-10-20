import { Bus, Inbox } from "../src/index.ts";

const bus = new Bus();
const inbox = new Inbox(8);
const first = bus.subscribe("a.b", inbox);
const second = bus.subscribe("a.b", inbox);
const closedFirst = first.close();
const closedSecond = second.close();
if (closedFirst !== true || closedSecond !== true || first.active() || second.active()) {
  throw new Error(`subdup leaked: ${closedFirst}/${closedSecond}`);
}
console.log("subdup:", closedFirst, closedSecond, first.active(), second.active());
