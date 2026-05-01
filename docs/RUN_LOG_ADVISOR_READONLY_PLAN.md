# Run Log Advisor (Read-Only) Implementation Plan

## Objective

Add a **read-only Run Log Advisor** that watches run events/logs and provides node-specific troubleshooting guidance without changing runtime behavior.

---

## Non-Negotiable Guardrails

1. Advisor must be **read-only** (no writes to scheduler, node bindings, queue runtime, run control).
2. Advisor must be **feature-flagged** and easy to disable.
3. Advisor must not alter node lifecycle/status projection logic.
4. Advisor output is suggestions only (no automatic retries, resets, reruns, pinning, or graph edits).
5. If advisor fails internally, run behavior remains unaffected.

---

## Scope

### In-scope
- Parse existing run monitor inputs (node rows + logs/events).
- Produce structured advisory items with:
	- rule id
	- severity
	- affected node ids
	- reason/evidence
	- suggested actions
- Surface in Run Monitor as a separate tab/panel.

### Out-of-scope
- Runtime mutation
- Auto-remediation
- Status/lifecycle reconciliation changes

---

## Data Contract

Create a small advisor contract module:

- `AdvisorySeverity = 'error' | 'warning' | 'info'`
- `AdvisoryItem`
	- `id: string` (stable key)
	- `ruleId: string`
	- `severity: AdvisorySeverity`
	- `title: string`
	- `nodeIds: string[]`
	- `evidence: string[]`
	- `explanation: string`
	- `actions: string[]`
	- `confidence: 'low' | 'medium' | 'high'`
	- `createdAt: string`

Stability rule for `id`:
- `id = ${ruleId}:${sortedNodeIds.join(',')}:${normalizedEvidenceHash}`
- Must remain stable between monitor ticks unless evidence actually changes.

---

## Implementation Steps

## Step 1 — Advisor Core (Pure function)

Create `buildRunAdvisory(input)` as a pure function.

Input:
- run status
- run monitor node rows
- recent logs/events (bounded)

Output:
- `AdvisoryItem[]`

Rules are declarative and side-effect free.

### Initial Rules (v1)
- `COMPONENT_OUTPUT_NOT_RESOLVED`
- `COMPONENT_OUTPUT_HANDLE_UNRESOLVED`
- `MODEL_EXECUTION_FAILED` timeout/retry patterns
- `WAITING_REQUIRED_INPUT` with zero pending/depth and no inflight (possible closure mismatch)
- provider queue saturation hints (`AWAITING_LEASE` prolonged)

---

## Step 2 — Feature Flag

Add a flag in monitor/config state:
- `runAdvisorEnabled: boolean` (default false)

Behavior:
- false: do not compute advisor.
- true: compute + render advisor.

---

## Step 3 — Bounded Log Window

Feed advisor only a bounded window (e.g., last N events/log lines per run).

Rules:
- Keep deterministic order.
- Deduplicate repeated evidence lines by node + code.

---

## Step 4 — UI Panel (Isolated)

Add a dedicated monitor tab/panel:
- Title: `Advisor`
- Summary chips: error/warning/info counts
- Collapsible items
- Node chips clickable to select node

Isolation rule:
- Panel reads advisor output only; no writes except optional `selectNode(nodeId)`.

---

## Step 5 — Diagnostics Hooks

Add lightweight diagnostics:
- advisor compute duration ms
- advisory item count
- last update timestamp

Must not write into run runtime state; keep local/UI diagnostics only.

---

## Step 6 — Fallback Safety

Wrap advisor compute in safe boundary:
- on exception -> return empty list + diagnostics warning
- never throw into run monitor rendering path

---

## Integration Test Plan

## IT1 — Read-only guarantee under active run

Scenario:
- Active run with synthetic logs triggering advisor rules.

Assert:
- Advisor items appear.
- No mutation to scheduler snapshot, queue runtime, node bindings, run status.
- No extra run events emitted by advisor.

## IT2 — Feature flag off

Scenario:
- Same run data, advisor disabled.

Assert:
- No advisor panel items.
- Monitor/live behavior unchanged.

## IT3 — Feature flag on

Scenario:
- Advisor enabled, known error present.

Assert:
- Correct advisory item rendered with node mapping and suggested actions.

## IT4 — Node selection wiring only

Scenario:
- Click node chip in advisor.

Assert:
- Node is selected in inspector/canvas.
- No runtime mutation side effects.

## IT5 — Fault isolation

Scenario:
- Force advisor rule throw.

Assert:
- Monitor still renders.
- Run continues unaffected.
- Advisor shows empty/diagnostic fallback.

---

## Regression Test Plan

## RT1 — Stable key regression

Given identical input across ticks:
- advisory item ids remain identical.

## RT2 — Rule evidence regression

For each rule:
- expected evidence line/code must appear in item.
- expected severity classification must match.

## RT3 — Drift prevention regression

Ensure enabling advisor does not change:
- node lifecycle
- node status display
- monitor grouping counts

Compare advisor off vs on snapshots for identical run input.

## RT4 — Performance regression

Bounded log input stress test:
- advisor compute stays under target threshold for N nodes / M log lines.

## RT5 — Duplicate suppression

Repeated identical errors in window:
- no duplicate advisory entries for same rule+node set.

---

## Rollout Plan

1. Land advisor core + tests behind flag (off by default).
2. Land UI tab behind same flag.
3. Run extended regression suite.
4. Enable in dev only.
5. Promote to default-on only after stability period.

---

## Suggested Commit Sequence

### Commit 1
```text
feat(monitor-advisor): add read-only advisory core behind feature flag

- introduce advisory data contract and pure buildRunAdvisory() engine
- add bounded log/event ingestion for advisor rules
- add initial rule set for common runtime failures and waiting anomalies
- keep advisor disabled by default
```

### Commit 2
```text
test(monitor-advisor): add integration and regression coverage for read-only advisor

- verify advisor does not mutate runtime/scheduler state
- verify flag-off/flag-on behavior and rule mapping
- add stable-key, dedupe, drift-prevention, and fault-isolation regressions
```

### Commit 3
```text
feat(monitor-ui): add Advisor tab for run guidance (read-only)

- add run monitor Advisor panel with severity summaries and item details
- wire node-chip navigation to node selection only
- add local diagnostics for advisor compute timing and update cadence
```

### Commit 4 (optional)
```text
chore(monitor-advisor): enable advisor in dev profile and document guardrails

- enable advisor by default in dev only
- document read-only constraints and rollback switch
```

---

## Rollback Strategy

- Single switch: set `runAdvisorEnabled=false`.
- UI tab hidden when disabled.
- Core module retained but inert.

---

## Acceptance Criteria

1. Advisor provides useful node-specific suggestions for known failures.
2. Runtime behavior is identical with advisor on/off (except advisory UI output).
3. No status/lifecycle/monitor grouping drift caused by advisor.
4. Integration + regression suites pass.
5. Advisor can be disabled instantly via feature flag.
