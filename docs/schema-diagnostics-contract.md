# Schema Diagnostics Contract (v1)

Shared contract file: `shared/schema_diagnostics.v1.json`

Current diagnostic codes:
- `TYPE_MISMATCH`
- `PAYLOAD_SCHEMA_MISMATCH`

Payload shape:
- `code`: machine-readable diagnostic code.
- `message`: human-readable message.
- `edgeId`: optional edge identifier.
- `nodeId`: optional node identifier.
- `details`: optional structured payload (`provided_schema`, `required_schema`, etc).
- `suggestions`: optional list of actionable next steps.

Frontend notes:
- Edge diagnostics emitted by `graphStore` use this code set directly.

Backend notes:
- `GraphValidator` emits codes from `app.runner.schema_diagnostics`.
- Runner tests assert emitted codes are in the shared contract.

## Diagnostic Transport Events (v2)

Transport events communicate diagnostic lifecycle over control/event channels, but do not own semantic authority.

Events:
- `diagnostic_raised`
- `diagnostic_cleared`

Payload fields:
- `key`: stable identity for dedupe/clear correlation.
- `edgeId`: associated edge id.
- `nodeId`: optional related node id.
- `source`: `contract_engine` (authoritative) or `schema_plane` (informational).
- `severity`: transport severity (`info|warning|error`) for timeline display.
- `code`: machine-readable code (`TYPE_MISMATCH`, `OPAQUE_DEPENDENCY`, etc).
- `message`: human-readable message.
- `details`: optional structured metadata.

Authority rules:
- Contract diagnostics (`clean|warning|error`) remain canonical for edge schema class rendering.
- Transport events are append/update lifecycle signals and must be processed idempotently.
- Out-of-order or replayed transport events must not override canonical semantic severity.
