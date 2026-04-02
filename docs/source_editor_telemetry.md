# Source Editor Telemetry

This document describes the frontend-only telemetry events emitted by Source editors for UX analysis.

## Event channel

- Browser event: `source-editor-telemetry`
- Payload: `CustomEvent.detail` with `SourceEditorTelemetryEvent` shape.

## Event types

- `section_toggle`
  - `sourceKind`, `nodeId`, `sectionId`, `open`
- `validation`
  - `sourceKind`, `nodeId`, `controlId`, `severity`, `action`
  - `action` is `shown` when a hint appears and `resolved` when it disappears.
- `auto_adjustment`
  - `sourceKind`, `nodeId`, `change`, `redactedContext`
  - Sensitive keys are redacted before emit.

## Privacy policy

- Raw credentials are never emitted.
- Keys matching `token|secret|password|key|connection_string|auth` are redacted.

## Suggested local analysis snippet

```js
const events = [];
window.addEventListener('source-editor-telemetry', (event) => {
	events.push(event.detail);
});
```

## Suggested aggregations

- Top sections toggled per `sourceKind`
- Validation hint churn rate:
  - `validation shown` minus `validation resolved`
- Auto-adjustments per source kind and per node
