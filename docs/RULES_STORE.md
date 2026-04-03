# Rules Store

Last updated: 2026-03-30
Owner: Product/UX + Runtime behavior rules captured from collaboration history.

## Purpose

This file is the persistent rule source for implementation behavior that must survive chat/session compaction.
When there is ambiguity, prefer these rules over ad-hoc assumptions.

## UI/Theming Rules

1. New dropdowns must use `ThemedSelect` by default (avoid native `<select>` for critical UX surfaces).
2. Existing problematic dropdowns in `NodeInspector` must stay on `ThemedSelect`:
	- Processing Policy
	- Port Declarations
	- Work Edges
	- Param Edges
3. Dark/light theme readability is mandatory for all dropdown menus and option lists.
4. Collapsed panes must consume minimal space (hard rule).
5. Sticky headers in Run Monitor lists must use opaque backgrounds so scrolling rows never show through.

## LLM Lease / Running Visual Truthfulness

1. A node must not emit `node_started` until all execution-critical resources are acquired (including LLM lease if required).
2. After lease release:
	- No running star
	- No running (blue dashed) work edges
	- Node may still be `busy` for post-processing if applicable, but not lease-running.
3. Running visuals are work-plane only.
4. Param/control/read-once edges must never be rendered as active/running.
5. Backend scheduler is authoritative for active execution truth.

## LLM Indicator Rules

1. Star indicates active LLM lease holder only.
2. Star placement: same line as model text, immediately left of model name.
3. If only one LLM lease exists, at most one star should be visible.
4. If multiple leases are configured, each active holder may show a star; count must reflect actual lease holders.

## Queue / Scheduler / Scope Rules

1. Removing pins must immediately remove/recompute any derived run scope.
2. Historical run records are immutable and must not leak into active planning state.
3. Active runtime state and historical state must stay clearly separated.
4. Resume/fallback reconciliation must converge to backend truth.

## Pinning Rules

1. Pin eligibility:
	- Node status must be `succeeded`
	- Node must have current bound artifact pair (`current.execKey` + `current.artifactId`)
2. Once pinned, upstream changes do not invalidate downstream execution from the pinned node.
3. Changing pinned node parameters/settings (except pin status itself) unpins the node and warns user via modal.
4. `Run from selected` on a pinned node uses the stored pinned artifact/snapshot contract.
5. Pin modes:
	- Per-run pin
	- Sticky pin (until manually unpinned)
6. Visual encoding:
	- Per-run pin: amber styling
	- Sticky pin: blue styling + `#` marker
	- If unpinned: no pin indicator
7. Pin controls should show current status and cycle to next state on click (not separate `pin!`, `pin#`, `unpin` buttons).

## Pause / Resume Phase 1 Policy

1. Pause and Reset are distinct semantics.
2. Pause is safe-boundary only (no hard interrupts, no mid-node continuation).
3. If non-resumable work is in-flight, run stays `pausing` until that work completes.
4. Resume is fail-fast on frontier/identity validation failure.
5. No selective branch invalidation in Phase 1.
6. Pause terminalization requires:
	- Admission closed
	- No active admitted work
	- No active leases
	- No non-resumable inflight work
	- Pause snapshot persisted successfully

## Pause Snapshot / Resume Validation Rules

1. Snapshot must capture authoritative, post-drain binding truth (not planned/default values).
2. Snapshot ordering:
	- Node finished
	- Bindings committed
	- Frontier basis captured
	- Snapshot persisted
	- Run status set `paused`
	- `run_paused` emitted
3. Use one centralized frontier-basis builder for both snapshot capture and resume validation.
4. Validation basis must include:
	- `graph_id`
	- `node_id`
	- node state hash
	- upstream artifact bindings/keys
	- determinism env hash
	- execution version
5. Resume failure diagnostics must be structured and include node id(s), changed fields, and reason code.

## Run Monitor / Inspector Layout Rules

1. Run Monitor should exist in one place only: the Run Monitor slideout.
2. Slideout is opened from the `Monitor` pill in NodeInspector.
3. Remove duplicate Run Monitor renderings from NodeInspector body.
4. Run Monitor contains Environment Variables section (collapsed by default).
5. There should be visual separation between Run Monitor and Environment sections.
6. Height splitters:
	- Respect minimum heights for all panes.
	- Growth stops when either:
		- target pane scrollbar disappears, or
		- opposite pane reaches minimum height.
	- Stop at whichever happens first.
7. Three scrollable panels (`nodes`, `edges`, `env vars`) should each fill available area in their section.

## Dataflow / Port Semantics Rules

1. Work, param, control planes are distinct and must be represented/handled distinctly.
2. Param/control edges may be shown as resolved/satisfied, but must not be treated as active running execution edges.
3. Preserve single-LLM fairness/lease semantics; scheduler behavior should remain transparent in logs/monitoring.

## Logging / UX Rules

1. Logs should remain append-only; pause must not clear run history.
2. During active runs, user manual log scrolling must be respected (no forced autoscroll lock unless user is at bottom).
3. Debug logging should not flood indefinitely after run completion.

## Artifact Retention Rules

1. Artifacts emitted during an active run must not be pruned mid-run.
2. Artifact pruning is run-scoped (not per-node output-count scoped).
3. Retention pruning executes only after run terminalization (`succeeded`/`failed`/`canceled`/`skipped`).
4. Artifact links emitted in run logs should remain valid through run completion for retained runs.

## Implementation Process Preferences

1. Ask clarifying questions before coding when requirements are ambiguous.
2. For implementation requests: implement + test thoroughly (not discuss-only unless asked).
3. Report clearly what changed and what was tested.

## Maintenance

1. When user states a persistent rule, append/update this file in the same PR/change set.
2. If a new rule conflicts with an older rule, update this file and mark old rule superseded.
3. Keep rule text concrete and testable.

## Hidden Items

Use this list for UI items intentionally hidden for now (possible future restore).

1. `Show Guided DS/ML` restore pill in the inspector sidebar is hidden.
2. `Ctrl+K` pill in the status bar is hidden.

## Toolbar Placement Rules

1. `Monitor` pill is placed in the status bar add-actions area, immediately to the right of `+ Add`.
