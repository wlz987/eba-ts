import { Inbox } from "../src/index.ts";

try {
  new Inbox(0);
} catch (error) {
  if (error instanceof Error && error.message === "capacity must be a positive integer") {
    console.log("refused:", error.message);
    process.exit(0);
  }
  throw error;
}
throw new Error("zero capacity should be refused");
