# Stream Close Classification Plan (Pause/Finish vs Real Transport Error)

## Objective

Avoid noisy/misleading log lines like:

- `Event stream error; reconciling run status`

when stream closure is expected after pause/finish terminalization.

Keep reconciliation behavior, but classify closure correctly with low coupling.

---

## Problem Summary

Current behavior conflates:

1. **Expected stream closure** after `run_paused` / `run_finished`
2. **Unexpected transport failure** during active run

Both currently trigger “error-like” messaging + reconciliation. Functionally safe, but UX-noisy and semantically inaccurate.

---

## Design Principles

1. **Transport-state and run-state are separate concerns**
2. **Run snapshot/reducer remains source of truth**
3. **Reconciliation remains idempotent safety net**
4. **User-facing severity should match certainty**

---

## Implementation Steps

## Step 1: Add explicit stream close classification

In run stream handling (store run manager module):

- Introduce stream close reason enum/state:
	- `expected_terminal_close`
	- `expected_pause_close`
	- `expected_manual_close`
	- `unexpected_close`
	- `transport_error`

Track lightweight context:

- last terminal run event type received (`run_paused`, `run_finished`, etc.)
- current run status at close time
- whether close was initiated locally (manual close path)

## Step 2: Route all stream shutdown through one closure handler

Unify stream `onerror`, `onclose`, and manual `close()` into a single classifier path.

Classifier rules (ordered):

1. If local close requested -> `expected_manual_close`
2. Else if last event is `run_paused` -> `expected_pause_close`
3. Else if last event is terminal (`run_finished`, `run_failed`, `run_canceled`) -> `expected_terminal_close`
4. Else if transport provided explicit error -> `transport_error`
5. Else -> `unexpected_close`

## Step 3: Keep reconciliation, but adjust log severity/message

For all closure classes, keep reconciliation poll behavior.

Logging changes:

- Expected close:
	- `Event stream closed; reconciling run status` (info)
- Unexpected close:
	- `Event stream closed unexpectedly; reconciling run status` (warn)
- Transport error:
	- `Event stream transport error; reconciling run status` (warn/error)

Do not emit “error” wording for expected pause/finish closures.

## Step 4: Preserve existing correctness guarantees

Do **not** alter:

- pause terminalization conditions
- run status reducer semantics
- polling reconciliation source of truth

Only change classification + messaging path.

## Step 5: Optional diagnostics hook (dev only)

Add one concise structured trace line on stream shutdown:

- `[stream-close] class=expected_pause_close run_id=... run_status=paused last_event=run_paused reconcile=true`

Guarded by existing debug/trace toggle if available.

---

## Test Plan

## A) Unit tests (run stream manager / reducer-adjacent tests)

1. **Pause terminal close classified expected**
	- simulate event sequence ending in `run_paused`, then close
	- assert class `expected_pause_close`
	- assert info-level log text (not error wording)
	- assert reconciliation invoked once

2. **Finished terminal close classified expected**
	- `run_finished` then close
	- class `expected_terminal_close`
	- info-level message

3. **Manual close classified expected**
	- local close path invoked
	- class `expected_manual_close`
	- no error wording

4. **Unexpected close while running**
	- run active, no terminal event, close
	- class `unexpected_close`
	- warn-level message
	- reconciliation invoked

5. **Transport error path**
	- explicit transport error callback
	- class `transport_error`
	- warn/error message
	- reconciliation invoked

## B) Regression tests (existing monitor/run store suites)

1. **Pause log wording regression**
	- verify logs do not include `Event stream error...` for expected pause close
	- verify expected “closed; reconciling” wording appears

2. **No behavior regression in run status**
	- pause -> reconcile still lands in `paused`
	- finished -> reconcile still lands in terminal status

3. **Single reconciliation trigger**
	- ensure no duplicate immediate polls from close+error double-path.

## C) Integration/manual checks

1. Run -> Pause
	- expect:
		- `[pause] terminalized ...`
		- `Event stream closed; reconciling run status`
		- `Run reconciled via immediate poll (paused)`

2. Run -> Finish
	- same expected-close wording, no “error” text

3. Simulated network interruption mid-run
	- expect unexpected/transport warning wording + reconcile

---

## Acceptance Criteria

- Expected pause/finish stream closure no longer logs “error”.
- Reconciliation still occurs and status converges correctly.
- Unexpected close still surfaces warning/error-level signal.
- No duplicated reconciliation polls.

---

## Suggested Commits

1. `refactor(stream): classify run stream close reasons in one handler`
2. `fix(run-logs): downgrade expected pause/finish stream closure from error wording`
3. `test(stream): cover expected vs unexpected close classification and reconcile behavior`

If done in one pass:

- `fix(stream-close): classify expected pause/finish closure and keep reconcile without error noise`

