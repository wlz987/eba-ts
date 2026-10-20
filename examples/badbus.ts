import { Bus, Inbox, InvalidTopic } from "../src/index.ts";

try {
  new Bus().subscribe("bad!", new Inbox(8));
} catch (error) {
  if (error instanceof InvalidTopic) {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("bad pattern should be refused by the bus");
