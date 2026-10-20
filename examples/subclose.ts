import { Bus, Inbox } from "../src/index.ts";

const sub = new Bus().subscribe("a.b", new Inbox(8));
const first = sub.close();
const second = sub.close();
if (first !== true || second !== false) {
  throw new Error(`subclose leaked: ${first}/${second}`);
}
console.log("subclose:", first, second);
