# Run Logs: Selection Filter + Ctrl-Click Time Context Overlay Plan

## Goal

Add two low-noise interactions in Run Logs:

1. **Selection-to-filter**  
	- User does `mousedown -> select text -> mouseup` in run logs.
	- Selected text is applied to existing log filter input.

2. **Ctrl-click time context**  
	- User `Ctrl+Click`s a log row/token.
	- Open a scrollable overlay centered on that clicked event.
	- Overlay supports scrolling up/down through surrounding events and has a close button.

No existing filter expression semantics should change.

---

## UX Contract

### A) Selection-to-filter

- Trigger: mouseup in log pane with a non-empty text selection.
- Action: set `runLogFilter` to selected text (trimmed).
- Ignore when:
	- `Ctrl` key is held.
	- selection is empty/whitespace.
	- user is selecting outside run logs container.

### B) Ctrl-click context overlay

- Trigger: `Ctrl+Click` on a log row.
- Action: open modal/overlay with:
	- title: `Log Context`
	- anchor metadata: timestamp + node id/name (if available) + run id (if available)
	- list of context events centered around clicked event
	- close button
	- `Esc` closes
- Context list:
	- initial window: e.g. `+/- 50` rows around anchor (config constant)
	- user can scroll to inspect surrounding events
	- anchor row visually highlighted

### C) Interaction precedence

- `Ctrl+Click` must **not** trigger selection-to-filter.
- Normal click (no selection) does nothing special.
- Selection-to-filter does not auto-open overlay.

---

## Implementation Steps

## 1) State + constants in `src/lib/flow/FlowCanvas.svelte`

Add local state:

- `runLogContextOpen: boolean`
- `runLogContextAnchorId: string | null`
- `runLogContextRows: LogEntry[]` (or the existing run log item type)
- `runLogContextAnchorIndex: number`
- `RUN_LOG_CONTEXT_RADIUS = 50` (or similar)

Derived helpers:

- `findLogIndexById(id)`
- `buildContextWindow(logs, anchorIndex, radius)`

## 2) Selection-to-filter wiring in run log pane

Within the logs container (`.logs`):

- Add `on:mousedown` to track selection start in logs.
- Add `on:mouseup` handler:
	- return early if `event.ctrlKey` true.
	- read `window.getSelection()?.toString()`.
	- trim and apply to `runLogFilter` if non-empty.

Guardrails:

- Ensure selection source belongs to logs container (using `contains` check).
- No change to filter parser or predicate logic.

## 3) Ctrl-click row handler

On each rendered log row (`.log`):

- Add click handler that checks `event.ctrlKey`.
- If false, return.
- If true:
	- `preventDefault()`
	- locate clicked row id/index in full log array
	- compute context window via helper
	- set overlay open state

## 4) Overlay UI

Add overlay markup in `FlowCanvas.svelte` (or small extracted panel component if preferred):

- Backdrop + content panel
- Header with `Log Context` and close button
- metadata line (anchor ts / node / run)
- scrollable list of context rows
- anchor row highlighted
- close on backdrop click optional (recommended yes)
- close on `Esc` key

Keep styles simple and aligned with current dark theme.

## 5) Accessibility + keyboard

- Close button has `aria-label="Close log context"`.
- Overlay has role `dialog` and `aria-modal="true"`.
- `Esc` closes.
- Focus close button on open (optional but recommended).

## 6) Non-goals / explicit exclusions

- Do not alter existing run log filter expression syntax.
- Do not add auto-sync between main log scroller and overlay scroller.
- Do not add new backend APIs; this is client-side behavior only.

---

## Regression and Integration Tests

## A) Unit/logic tests (new or existing test file for log interactions)

1. **Selection applies filter**
	- simulate selection text + mouseup in logs
	- assert `runLogFilter` updated

2. **Selection ignored on ctrl**
	- simulate ctrl+mouseup with selection
	- assert `runLogFilter` unchanged

3. **Whitespace selection ignored**
	- selection is spaces/newlines only
	- assert no filter change

4. **Context window centered**
	- given known log array and anchor index
	- assert window includes expected rows and correct highlighted anchor

5. **Ctrl-click opens overlay**
	- simulate ctrl-click on row
	- assert overlay open + metadata populated

6. **Normal click does not open overlay**
	- click without ctrl
	- assert overlay remains closed

7. **Esc closes overlay**
	- open overlay, dispatch Escape
	- assert closed

## B) UI integration tests

1. **Selection filter + count update**
	- select token from log message
	- filter input updates
	- `Logs: N` reflects filtered set

2. **Ctrl-click context browse flow**
	- ctrl-click log row
	- overlay appears with surrounding rows
	- scroll inside overlay
	- close via button
	- verify underlying filter untouched

3. **No action collision**
	- ctrl-click should not trigger selection filter behavior
	- selection filter should not auto-open overlay

## C) Manual verification checklist

- Select token in logs -> filter updates immediately.
- Ctrl-click on error line -> context overlay opens centered at that line.
- Scroll overlay up/down works.
- Close button and Esc both close overlay.
- Main logs remain visible and unchanged after closing overlay.

---

## Suggested Commit Sequence

1. `feat(run-logs): add selection-to-filter in log pane`
2. `feat(run-logs): add ctrl-click context overlay for surrounding events`
3. `test(run-logs): cover selection filter and context overlay interactions`

---

## Risks and Mitigations

- **Accidental filter updates during text selection**
	- Mitigate with trim/empty checks and logs-container containment checks.
- **Large log arrays impact overlay rendering**
	- Render only context window, not entire log stream.
- **Interaction confusion**
	- Add small tooltip/help text: “Select text to filter. Ctrl+Click for context.”

