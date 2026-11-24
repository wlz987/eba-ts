
import { Envelope } from "#src/core/envelope.js";

export interface TopicSchema {
  readonly version: number;
  validate(payload: unknown): string | null;
  migrate?: (payload: unknown, fromVersion: number) => unknown;
}

export class SchemaRegistry {
  #topics = new Map<string, TopicSchema>();

  register(topic: string, schema: TopicSchema): void {
    const existing = this.#topics.get(topic);
    if (existing !== undefined && schema.version <= existing.version) {
      throw new Error(`schema ${topic} is already at version ${existing.version}`);
    }
    this.#topics.set(topic, schema);
  }

  check(topic: string, payload: unknown): number {
    const key = topic;
    const schema = this.#topics.get(key);
    if (schema === undefined) return 1;
    const error = schema.validate(payload);
    if (error !== null) throw new Error(`invalid ${key}@${schema.version}: ${error}`);
    return schema.version;
  }

  /** Migrate an older payload; a throwing migrator is a migration failure. */
  migrate(
    topic: string,
    payload: unknown,
    fromVersion: number,
  ): { readonly version: number; readonly payload: unknown } {
    const key = topic;
    const schema = this.#topics.get(key);
    if (schema === undefined) throw new Error(`no schema for ${key}`);
    if (fromVersion > schema.version)
      throw new Error(
        `unsupported future schema version for ${key}: ${fromVersion}`,
      );
    if (fromVersion === schema.version)
      return { version: schema.version, payload };
    if (schema.migrate === undefined)
      throw new Error(`no migration for ${key}@${fromVersion} -> ${schema.version}`);
    let migrated: unknown;
    try {
      migrated = schema.migrate(payload, fromVersion);
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      const failure = new Error(
        `invalid ${key}@${schema.version} after migration: ${reason}`,
      );
      failure.cause = cause;
      throw failure;
    }
    const error = schema.validate(migrated);
    if (error !== null)
      throw new Error(`invalid ${key}@${schema.version} after migration: ${error}`);
    return { version: schema.version, payload: migrated };
  }

  version(topic: string): number {
    return this.#topics.get(topic)?.version ?? 1;
  }

  /** Migrate an envelope to this registry's current version, fail-closed. */
  migrateEnvelope(envelope: Envelope): Envelope {
    const topic = envelope.header.topic;
    const current = this.version(topic);
    const from = envelope.header.sv;
    if (from === current) return envelope;
    const migrated = this.migrate(topic, envelope.payload, from);
    return new Envelope(
      { ...envelope.header, sv: migrated.version },
      migrated.payload,
    );
  }

  get size(): number {
    return this.#topics.size;
  }
}
