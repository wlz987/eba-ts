export type EnvelopeId = string & { readonly __brand: "EnvelopeId" };
export type ActorId = string & { readonly __brand: "ActorId" };

export function envelopeId(raw: string): EnvelopeId {
  if (typeof raw !== "string")
    throw new TypeError("envelope id must be a string");
  return raw as EnvelopeId;
}

export function actorId(raw: string): ActorId {
  if (typeof raw !== "string") throw new TypeError("actor id must be a string");
  return raw as ActorId;
}
