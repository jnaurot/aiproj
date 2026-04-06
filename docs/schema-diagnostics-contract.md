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
