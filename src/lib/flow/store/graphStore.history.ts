// src/lib/flow/store/graphStore.history.ts
//
// Self-contained undo/redo history stack for the graph store.
//
// Design
// ──────
// The history manager is created once inside the graphStore IIFE via
// `createHistoryManager(deps)`.  It returns:
//
//   • `pushSnapshot`          – called by the wrapped `update` fn after every
//                               successful state transition (not exported to UI).
//   • `resetToSnapshot`       – called by hardResetGraph / applyGraphDocument /
//                               clearHistory when the stack must be wiped.
//   • `wrapUpdate`            – wraps the raw Svelte `update` so that every
//                               mutation automatically pushes a history entry
//                               and runs the audit callback.  The orchestrator
//                               replaces its own `update` reference with this.
//   • `actions`               – the public API surface re-exported by graphStore
//                               (canUndo, canRedo, undo, redo, …).
//
// Dependencies (injected, no direct imports from graphStore.ts)
// ─────────────────────────────────────────────────────────────
//   getState         – returns the current GraphState snapshot.
//   applyDocument    – applies a raw {nodes, edges} document to the store;
//                      owned by the graph-edit module, injected here so history
//                      can restore snapshots without knowing how parsing works.
//   auditTransition  – called with (prev, next, ctx) after every update; owned
//                      by graphStore.audit (or inlined in the orchestrator).

import type { PipelineGraphDTO } from '$lib/flow/types';
import type { GraphState, AuditContext } from './graphStore.types';

// ---------------------------------------------------------------------------
// Public surface types
// ---------------------------------------------------------------------------

export type HistoryActions = {
	canUndo(): boolean;
	canRedo(): boolean;
	setHistoryLimit(limit: number): void;
	clearHistory(): void;
	beginHistoryTransaction(): void;
	endHistoryTransaction(): void;
	undo(): { ok: boolean; reason?: string };
	redo(): { ok: boolean; reason?: string };
};

/** The full object returned by createHistoryManager. */
export type HistoryManager = {
	/** Push a new snapshot onto the stack.  No-op when a transaction is open or
	 *  a undo/redo application is in flight. */
	pushSnapshot(snapshot: PipelineGraphDTO): void;
	/** Wipe the stack and set `present` to `snapshot`. */
	resetToSnapshot(snapshot: PipelineGraphDTO): void;
	/** Returns true while an undo/redo restoration is in progress.  Used by
	 *  `applyGraphDocument` to suppress the automatic stack push it would
	 *  otherwise trigger. */
	isApplying(): boolean;
	/** Wraps the raw Svelte `rawUpdate` to automatically push history and run
	 *  the audit callback after every successful mutation. */
	wrapUpdate(
		rawUpdate: (recipe: (s: GraphState) => GraphState) => void,
		auditTransition: (prev: GraphState, next: GraphState, ctx: AuditContext) => void,
		snapshotFromState: (s: GraphState) => PipelineGraphDTO
	): (recipe: (s: GraphState) => GraphState, ctx?: AuditContext) => void;
	/** The public action methods forwarded by graphStore. */
	actions: HistoryActions;
};

// ---------------------------------------------------------------------------
// Dependencies injected at construction time
// ---------------------------------------------------------------------------

type HistoryDeps = {
	/** Read the current store state without subscribing. */
	getState(): GraphState;
	/** Apply a raw graph document – owned by the graph-edit module.
	 *  Returns whether the application succeeded. */
	applyDocument(
		graph: { nodes: unknown[]; edges: unknown[] },
		graphId: string | null
	): boolean;
	/** Convert current state to a DTO snapshot. */
	snapshotFromState(state: GraphState): PipelineGraphDTO;
};

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

const HISTORY_LIMIT_DEFAULT = 100;

/** Stable JSON serialisation used as a cheap equality key. */
function snapshotKey(snapshot: PipelineGraphDTO): string {
	try {
		return JSON.stringify(snapshot ?? null);
	} catch {
		return '';
	}
}

export function createHistoryManager(deps: HistoryDeps): HistoryManager {
	const { getState, applyDocument, snapshotFromState } = deps;

	// ── mutable state ──────────────────────────────────────────────────────
	let historyLimit = HISTORY_LIMIT_DEFAULT;
	let historyPast: PipelineGraphDTO[] = [];
	let historyFuture: PipelineGraphDTO[] = [];
	let historyPresent: PipelineGraphDTO = snapshotFromState(getState());

	// Guards that suppress automatic stack pushes in special situations.
	let applying = false;          // true while undo/redo restore is in flight
	let txnDepth = 0;              // >0 while a history transaction is open
	let txnStartKey: string | null = null;

	// ── private helpers ────────────────────────────────────────────────────

	function trimPast(): void {
		if (historyPast.length > historyLimit) {
			historyPast = historyPast.slice(historyPast.length - historyLimit);
		}
	}

	function pushSnapshot(snapshot: PipelineGraphDTO): void {
		if (applying) return;
		if (txnDepth > 0) return;
		if (snapshotKey(snapshot) === snapshotKey(historyPresent)) return;
		historyPast = [...historyPast, historyPresent];
		trimPast();
		historyPresent = snapshot;
		historyFuture = [];
	}

	function resetToSnapshot(snapshot: PipelineGraphDTO): void {
		historyPast = [];
		historyFuture = [];
		historyPresent = snapshot;
	}

	// ── transaction helpers ────────────────────────────────────────────────

	function beginTxn(): void {
		if (txnDepth === 0) {
			txnStartKey = snapshotKey(historyPresent);
		}
		txnDepth += 1;
	}

	function endTxn(): void {
		if (txnDepth <= 0) return;
		txnDepth -= 1;
		if (txnDepth > 0) return;

		// Commit: if the graph changed during the transaction, push one entry.
		const current = snapshotFromState(getState());
		const currentKey = snapshotKey(current);
		if (txnStartKey != null && currentKey !== txnStartKey) {
			historyPast = [...historyPast, historyPresent];
			trimPast();
			historyPresent = current;
			historyFuture = [];
		}
		txnStartKey = null;
	}

	// ── undo / redo ────────────────────────────────────────────────────────

	function applySnapshot(snapshot: PipelineGraphDTO): boolean {
		const nodes = Array.isArray((snapshot as any)?.nodes)
			? ((snapshot as any).nodes as unknown[])
			: [];
		const edges = Array.isArray((snapshot as any)?.edges)
			? ((snapshot as any).edges as unknown[])
			: [];
		const graphId = String((snapshot as any)?.meta?.graphId ?? '').trim() || null;

		applying = true;
		try {
			const ok = applyDocument({ nodes, edges }, graphId);
			if (!ok) return false;
			// After restoration, re-derive present from what is now in the store.
			historyPresent = snapshotFromState(getState());
			return true;
		} finally {
			applying = false;
		}
	}

	// ── wrapUpdate ─────────────────────────────────────────────────────────

	function wrapUpdate(
		rawUpdate: (recipe: (s: GraphState) => GraphState) => void,
		auditTransition: (prev: GraphState, next: GraphState, ctx: AuditContext) => void,
		snapFromState: (s: GraphState) => PipelineGraphDTO
	) {
		return (
			recipe: (state: GraphState) => GraphState,
			ctx: AuditContext = { source: 'unknown' }
		): void => {
			rawUpdate((state) => {
				const next = recipe(state);
				auditTransition(state, next, ctx);
				pushSnapshot(snapFromState(next));
				return next;
			});
		};
	}

	// ── public actions ─────────────────────────────────────────────────────

	const actions: HistoryActions = {
		canUndo(): boolean {
			return historyPast.length > 0;
		},

		canRedo(): boolean {
			return historyFuture.length > 0;
		},

		setHistoryLimit(limit: number): void {
			historyLimit = Math.max(1, Number(limit) || HISTORY_LIMIT_DEFAULT);
			if (historyPast.length > historyLimit) {
				historyPast = historyPast.slice(historyPast.length - historyLimit);
			}
		},

		clearHistory(): void {
			resetToSnapshot(snapshotFromState(getState()));
		},

		beginHistoryTransaction(): void {
			beginTxn();
		},

		endHistoryTransaction(): void {
			endTxn();
		},

		undo(): { ok: boolean; reason?: string } {
			if (historyPast.length === 0) return { ok: false, reason: 'at_oldest' };
			const target = historyPast[historyPast.length - 1];
			const current = historyPresent;
			if (!applySnapshot(target)) return { ok: false, reason: 'restore_failed' };
			historyPast = historyPast.slice(0, historyPast.length - 1);
			historyFuture = [...historyFuture, current];
			return { ok: true };
		},

		redo(): { ok: boolean; reason?: string } {
			if (historyFuture.length === 0) return { ok: false, reason: 'at_newest' };
			const target = historyFuture[historyFuture.length - 1];
			const current = historyPresent;
			if (!applySnapshot(target)) return { ok: false, reason: 'restore_failed' };
			historyFuture = historyFuture.slice(0, historyFuture.length - 1);
			historyPast = [...historyPast, current];
			trimPast();
			return { ok: true };
		},
	};

	// ── exported manager ───────────────────────────────────────────────────

	return {
		pushSnapshot,
		resetToSnapshot,
		isApplying: () => applying,
		wrapUpdate,
		actions,
	};
}

// ---------------------------------------------------------------------------
// Convenience: the transaction helper used by graph-edit actions (setSourceKind
// etc.) so they can wrap multi-step mutations in a single history entry.
// ---------------------------------------------------------------------------

/**
 * Run `fn` inside a history transaction using an already-constructed manager.
 * The transaction guarantees that any number of `update()` calls inside `fn`
 * produce exactly one undo entry, provided the graph actually changed.
 */
export function runInHistoryTransaction<T>(
	manager: HistoryManager,
	fn: () => T
): T {
	manager.actions.beginHistoryTransaction();
	try {
		return fn();
	} finally {
		manager.actions.endHistoryTransaction();
	}
}
