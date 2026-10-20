import { Bus, Inbox } from "../src/index.ts";

const sub = new Bus().subscribe("a.b", new Inbox(8));
sub.disarm();
const active = sub.active();
const closed = sub.close();
if (active !== false || closed !== false) {
  throw new Error(`subdisarm leaked: ${active}/${closed}`);
}
console.log("subdisarm:", active, closed);
