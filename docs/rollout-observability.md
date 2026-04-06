# Rollout Observability (TKT-025)

Use `GET /runs/diagnostics` as the near-real-time rollout feed.

## Response Fields

- `featureFlags.STRICT_SCHEMA_EDGE_CHECKS`: strict edge contract enforcement status.
- `featureFlags.STRICT_COERCION_POLICY`: strict coercion policy status.
- `rolloutMetrics.schemaFailures`: count of schema/contract failures seen by runtime.
- `rolloutMetrics.coercionApplied`: count of coercion applications (`[COERCION_APPLIED]` logs).
- `rolloutMetrics.componentBindingFailures`: count of component output binding failures.
- `rolloutMetrics.lastUpdatedAt`: ISO timestamp of the latest metric update.

## Quick Health Checks

1. Strict flags active:
`curl -s http://127.0.0.1:8000/runs/diagnostics | jq '.featureFlags'`

2. Failure/coercion trend:
`curl -s http://127.0.0.1:8000/runs/diagnostics | jq '.rolloutMetrics'`

3. Active run pressure:
`curl -s http://127.0.0.1:8000/runs/diagnostics | jq '.activeRuns | length'`

## Rollout Guidance

- If `schemaFailures` spikes after enabling strict checks, roll back with:
  - `STRICT_SCHEMA_EDGE_CHECKS=0`
  - `STRICT_COERCION_POLICY=0`
- Monitor `coercionApplied` while strict checks are off; use it to prioritize contract fixes before re-enabling strict mode.
