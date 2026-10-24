export { Envelope, makeEnvelope, snapshotEnvelope } from "./envelope.ts";
export type { EnvelopeLike, MakeEnvelopeOptions } from "./envelope.ts";
export { EnvelopeBuildError } from "./errors.ts";
export {
  copyHeader,
  diagnosticAddress,
  eventAddress,
  kindOf,
  makeHeader,
  requestAddress,
  resultAddress,
  validateHeader,
} from "./header.ts";
export type {
  Address,
  DiagnosticAddress,
  EventAddress,
  Header,
  Kind,
  RequestAddress,
  ResultAddress,
} from "./header.ts";
export { actorId, envelopeId } from "./ids.ts";
export type { ActorId, EnvelopeId } from "./ids.ts";
export { SeqIdGen, UuidIdGen } from "./idgen.ts";
export type { IdGen } from "./idgen.ts";
export { Matchmaker } from "./reply.ts";
export {
  Completion,
  errPayload,
  errPayloadCode,
  hasResultShape,
  isResultErr,
  isResultOk,
  okPayload,
  resultBody,
  resultCode,
  resultError,
  resultRequestId,
  resultValue,
} from "./result.ts";
export { isName, splitTopic } from "./topic.ts";
