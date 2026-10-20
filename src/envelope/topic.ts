import { EnvelopeBuildError } from "./errors.ts";

const NAME = /^[a-z][a-z0-9_]*$/;

export function isName(segment: unknown): segment is string {
  return typeof segment === "string" && NAME.test(segment);
}

export function splitTopic(topic: string): string[] {
  if (typeof topic !== "string" || !topic)
    throw new EnvelopeBuildError("topic must be a nonempty string");
  const parts = topic.split(".");
  for (const part of parts) {
    if (!isName(part))
      throw new EnvelopeBuildError(
        `illegal topic segment: ${JSON.stringify(part)}`,
      );
  }
  return parts;
}
