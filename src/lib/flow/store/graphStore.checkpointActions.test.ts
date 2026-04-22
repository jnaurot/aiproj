import { get } from 'svelte/store';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { KnownRunEvent } from '$lib/flow/types/run';

const createRunMock = vi.fn();
const getRunMock = vi.fn();
const streamRunEventsMock = vi.fn();

vi.mock('$lib/flow/client/runs', async () => {
	const actual = await vi.importActual<typeof import('$lib/flow/client/runs')>('$lib/flow/client/runs');
	return {
		...actual,
		createRun: (...args: any[]) => createRunMock(...args),
		getRun: (...args: any[]) => getRunMock(...args),
		streamRunEvents: (...args: any[]) => streamRunEventsMock(...args)
	};
});

import { graphStore } from './graphStore';

function installSingleNodeGraph(nodeId: string): void {
	graphStore.loadGraphDocument({
		nodes: [
			{
				id: nodeId,
				type: 'source',
				position: { x: 0, y: 0 },
				data: {
					kind: 'source',
					label: 'Source',
					sourceKind: 'text',
					params: {}
				}
			}
		],
		edges: []
	});
}

function makeSnapshot(graphId: string, nodeId: string, runId: string, withMemo = true) {
	return {
		graphId,
		status: 'succeeded',
		runId,
		runMode: 'from_start',
		plannedNodeIds: [nodeId],
		nodeBindings: {
			[nodeId]: {
				status: 'succeeded_up_to_date',
				isUpToDate: true,
				cacheValid: true,
				currentRunId: runId,
				lastRunId: runId,
				current: { execKey: 'exec-1', artifactId: 'art-1' },
				last: { execKey: 'exec-1', artifactId: 'art-1' },
				outputLineage: {
					out: { execKey: 'exec-1', artifactId: 'art-1' }
				},
				...(withMemo
					? {
							memoState: {
								decision: 'compute',
								memoKey: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
							}
						}
					: {})
			}
		}
	};
}

function makeCachedSnapshotWithoutMemo(graphId: string, nodeId: string, runId: string) {
	return {
		graphId,
		status: 'succeeded',
		runId,
		runMode: 'from_start',
		plannedNodeIds: [nodeId],
		nodeBindings: {
			[nodeId]: {
				status: 'succeeded_up_to_date',
				isUpToDate: true,
				cacheValid: true,
				currentRunId: runId,
				lastRunId: runId,
				current: {
					execKey: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
					artifactId: 'art-cached'
				},
				last: {
					execKey: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
					artifactId: 'art-cached'
				},
				outputLineage: {
					out: {
						execKey: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
						artifactId: 'art-cached'
					}
				}
			}
		}
	};
}

function makeCachedSnapshotWithoutMemoNonHexExecKey(graphId: string, nodeId: string, runId: string) {
	return {
		graphId,
		status: 'succeeded',
		runId,
		runMode: 'from_start',
		plannedNodeIds: [nodeId],
		nodeBindings: {
			[nodeId]: {
				status: 'succeeded_up_to_date',
				isUpToDate: true,
				cacheValid: true,
				currentRunId: runId,
				lastRunId: runId,
				current: {
					execKey: 'ek_cached_nonhex',
					artifactId: 'art-cached-nonhex'
				},
				last: {
					execKey: 'ek_cached_nonhex',
					artifactId: 'art-cached-nonhex'
				},
				outputLineage: {
					out: {
						execKey: 'ek_cached_nonhex',
						artifactId: 'art-cached-nonhex'
					}
				}
			}
		}
	};
}

function makeIdleCachedSnapshotWithMemo(graphId: string, nodeId: string, runId: string) {
	return {
		graphId,
		status: 'succeeded',
		runId,
		runMode: 'from_start',
		plannedNodeIds: [nodeId],
		nodeBindings: {
			[nodeId]: {
				status: 'idle',
				isUpToDate: true,
				cacheValid: true,
				currentRunId: runId,
				lastRunId: runId,
				current: { execKey: 'exec-idle', artifactId: 'art-idle' },
				last: { execKey: 'exec-idle', artifactId: 'art-idle' },
				outputLineage: {
					out: { execKey: 'exec-idle', artifactId: 'art-idle' }
				},
				memoState: {
					decision: 'reuse',
					memoKey: 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'
				}
			}
		}
	};
}

describe('graphStore checkpoint actions', () => {
	beforeEach(() => {
		createRunMock.mockReset();
		getRunMock.mockReset();
		streamRunEventsMock.mockReset();
		graphStore.hardResetGraph();
		graphStore.clearHistory();
	});

	it('createCheckpoint requires a bound artifact lineage', () => {
		installSingleNodeGraph('n1');
		const result = graphStore.createCheckpoint('n1', 'checkpoint 1');
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.toLowerCase()).toContain('artifact');
		}
	});

	it('createCheckpoint allows idle node when cached lineage + memo fingerprint are valid', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-idle-cached-valid';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeIdleCachedSnapshotWithMemo(graphId, nodeId, runId));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		const binding = (get(graphStore).nodeBindings as any)?.[nodeId];
		expect(String(binding?.status ?? '')).toBe('idle');
		const result = graphStore.createCheckpoint(nodeId, 'checkpoint idle');
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.checkpoint.artifactId).toBe('art-idle');
			expect(result.checkpoint.fingerprintAtCreation).toBe(
				'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'
			);
		}
	});

	it('createCheckpoint requires memo fingerprint', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-no-memo';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeSnapshot(graphId, nodeId, runId, false));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		const result = graphStore.createCheckpoint(nodeId, 'checkpoint 1');
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toContain('fingerprint');
		}
	});

	it('createCheckpoint rejects cached artifact lineage when memoState is missing', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-cached-no-memo';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeCachedSnapshotWithoutMemo(graphId, nodeId, runId));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		const result = graphStore.createCheckpoint(nodeId, 'checkpoint cached');
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toContain('fingerprint');
		}
	});

	it('createCheckpoint rejects cached succeeded artifact when lineage fingerprint is not 64-hex', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-cached-nonhex-exec';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeCachedSnapshotWithoutMemoNonHexExecKey(graphId, nodeId, runId));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		const result = graphStore.createCheckpoint(nodeId, 'checkpoint nonhex');
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toContain('fingerprint');
		}
	});

	it('createCheckpoint allows cached succeeded artifact with non-hex lineage when memo fingerprint exists', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-cached-nonhex-with-memo';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		const snapshot = makeCachedSnapshotWithoutMemoNonHexExecKey(graphId, nodeId, runId) as any;
		snapshot.nodeBindings[nodeId].memoState = {
			decision: 'reuse',
			memoKey: 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'
		};
		getRunMock.mockResolvedValue(snapshot);
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		const result = graphStore.createCheckpoint(nodeId, 'checkpoint memo');
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.checkpoint.artifactId).toBe('art-cached-nonhex');
			expect(result.checkpoint.fingerprintAtCreation).toBe(
				'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'
			);
		}
	});

	it('createCheckpoint stores registry entry and removeCheckpoint deletes it', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-with-memo';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeSnapshot(graphId, nodeId, runId, true));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		const created = graphStore.createCheckpoint(nodeId, 'checkpoint 1', 'desc');
		expect(created.ok).toBe(true);
		if (created.ok) {
			expect(created.checkpoint.nodeId).toBe(nodeId);
		}
		expect((get(graphStore).checkpointRegistry as any)?.[nodeId]?.name).toBe('checkpoint 1');
		expect((get(graphStore).nodeBindings as any)?.[nodeId]?.checkpointable).toBe(false);

		graphStore.removeCheckpoint(nodeId);
		expect((get(graphStore).checkpointRegistry as any)?.[nodeId]).toBeUndefined();
	});

	// ── Checkpoint-reuse event-sequence regression tests ─────────────────────────
	//
	// These tests replay the EXACT event sequence that the backend emits for a node
	// served via the trusted-checkpoint path (run.py lines 4992-5204).  They are the
	// conclusive proof for why "Save Checkpoint" is absent after such a run.
	//
	// The checkpoint path emits, in order:
	//   run_started  → clears memoState for planned nodes
	//   cache_decision (decision="cache_hit", execKey=pinned_exec_key)
	//   node_output  (artifactId=...)
	//   node_finished (status="succeeded", cached=true)
	//   *** _emit_memo_trace is NEVER called — the path returns early at line 5199 ***
	//
	// Consequence: memoState.memoKey is undefined after the run, so Save Checkpoint
	// is blocked regardless of lineage.execKey value.

	it('checkpoint-reuse run with pinned: fallback execKey leaves memoState undefined and blocks Save Checkpoint', async () => {
		// This is the exact failing scenario for Model_Spanish_Summary:
		// the stored checkpoint has no valid execKey so pinned_exec_key falls back
		// to the string "pinned:<nodeId>", which is not 64-hex.
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-checkpoint-reuse-pinned-fallback';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		// Snapshot carries nothing — the run stream drives all binding updates.
		getRunMock.mockResolvedValue({ graphId, status: 'succeeded', runId, nodeBindings: {} });

		const PINNED_EXEC_KEY = `pinned:${nodeId}`; // backend fallback string (non-hex)
		const ARTIFACT_ID = 'art-checkpoint-reused';

		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() => {
				// 1. run_started: node is planned → memoState is cleared
				onEvent({
					type: 'run_started',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					runMode: 'from_start',
					plannedNodeIds: [nodeId]
				} as KnownRunEvent);
				// 2. cache_decision: backend binds the pinned exec key — NO memo trace
				onEvent({
					type: 'cache_decision',
					runId: rid,
					at: '2026-04-10T00:00:00.010Z',
					nodeId,
					decision: 'cache_hit',
					execKey: PINNED_EXEC_KEY,
					artifactId: ARTIFACT_ID
				} as KnownRunEvent);
				// 3. node_output: artifact confirmed
				onEvent({
					type: 'node_output',
					runId: rid,
					at: '2026-04-10T00:00:00.020Z',
					nodeId,
					artifactId: ARTIFACT_ID,
					handle: 'out',
					cached: true
				} as KnownRunEvent);
				// 4. node_finished: status = succeeded
				onEvent({
					type: 'node_finished',
					runId: rid,
					at: '2026-04-10T00:00:00.030Z',
					nodeId,
					status: 'succeeded',
					cached: true,
					execution_time_ms: 5
				} as KnownRunEvent);
				// 5. run_finished: run ends — NO _emit_memo_trace was ever sent
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00.040Z',
					status: 'succeeded'
				} as KnownRunEvent);
			});
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');

		const binding = (get(graphStore).nodeBindings as any)?.[nodeId];

		// Artifact and status are present — the node definitely completed.
		expect(binding?.status).toMatch(/^succeeded/);
		expect(binding?.current?.artifactId ?? binding?.currentArtifactId).toBe(ARTIFACT_ID);

		// memoState MUST be undefined: run_started cleared it, no memo trace repopulated it.
		expect(binding?.memoState).toBeUndefined();

		// lineage.execKey is the pinned fallback string — not 64-hex.
		const lineageExecKey =
			binding?.current?.execKey ?? binding?.currentExecKey ?? '';
		expect(/^[0-9a-f]{64}$/i.test(lineageExecKey)).toBe(false);
		expect(lineageExecKey).toBe(PINNED_EXEC_KEY); // confirms it IS the fallback

		// createCheckpoint must reject: no valid fingerprint from either path.
		const result = graphStore.createCheckpoint(nodeId, 'should fail');
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toContain('fingerprint');
		}
	});

	it('checkpoint-reuse run with valid 64-hex stored execKey still blocks Save Checkpoint without memo trace', async () => {
		// Even with a valid cached lineage execKey, fingerprint authority is memoKey-only.
		// Without memo trace repopulation, Save Checkpoint must remain unavailable.
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-checkpoint-reuse-valid-execkey';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue({ graphId, status: 'succeeded', runId, nodeBindings: {} });

		// A proper 64-hex exec key — what a valid stored checkpoint would have.
		const VALID_EXEC_KEY = 'c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1';
		const ARTIFACT_ID = 'art-checkpoint-valid';

		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() => {
				onEvent({ type: 'run_started', runId: rid, at: '2026-04-10T00:00:00Z', runMode: 'from_start', plannedNodeIds: [nodeId] } as KnownRunEvent);
				// cache_decision with valid exec key — still NO memo trace
				onEvent({ type: 'cache_decision', runId: rid, at: '2026-04-10T00:00:00.010Z', nodeId, decision: 'cache_hit', execKey: VALID_EXEC_KEY, artifactId: ARTIFACT_ID } as KnownRunEvent);
				onEvent({ type: 'node_output', runId: rid, at: '2026-04-10T00:00:00.020Z', nodeId, artifactId: ARTIFACT_ID, handle: 'out', cached: true } as KnownRunEvent);
				onEvent({ type: 'node_finished', runId: rid, at: '2026-04-10T00:00:00.030Z', nodeId, status: 'succeeded', cached: true, execution_time_ms: 5 } as KnownRunEvent);
				onEvent({ type: 'run_finished', runId: rid, at: '2026-04-10T00:00:00.040Z', status: 'succeeded' } as KnownRunEvent);
			});
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');

		const binding = (get(graphStore).nodeBindings as any)?.[nodeId];

		// memoState is still undefined — no memo trace fired for checkpoint reuse.
		expect(binding?.memoState).toBeUndefined();

		// Lineage.execKey may be valid 64-hex, but it is not authoritative for checkpoint fingerprinting.
		const lineageExecKey = binding?.current?.execKey ?? binding?.currentExecKey ?? '';
		expect(/^[0-9a-f]{64}$/i.test(lineageExecKey)).toBe(true);

		const result = graphStore.createCheckpoint(nodeId, 'valid checkpoint');
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toContain('fingerprint');
		}
	});

	it('adding a memo trace to the checkpoint-reuse sequence fixes Save Checkpoint', async () => {
		// This test proves what the missing _emit_memo_trace call would fix:
		// if the backend DID emit a memo trace during the checkpoint path, memoState
		// would be populated and the fingerprint check would pass regardless of the
		// stored execKey value.
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-checkpoint-reuse-with-memo';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue({ graphId, status: 'succeeded', runId, nodeBindings: {} });

		const PINNED_EXEC_KEY = `pinned:${nodeId}`; // still non-hex stored execKey
		const ARTIFACT_ID = 'art-checkpoint-with-memo';
		const MEMO_KEY = 'd2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2';

		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() => {
				onEvent({ type: 'run_started', runId: rid, at: '2026-04-10T00:00:00Z', runMode: 'from_start', plannedNodeIds: [nodeId] } as KnownRunEvent);
				onEvent({ type: 'cache_decision', runId: rid, at: '2026-04-10T00:00:00.010Z', nodeId, decision: 'cache_hit', execKey: PINNED_EXEC_KEY, artifactId: ARTIFACT_ID } as KnownRunEvent);
				onEvent({ type: 'node_output', runId: rid, at: '2026-04-10T00:00:00.020Z', nodeId, artifactId: ARTIFACT_ID, handle: 'out', cached: true } as KnownRunEvent);
				// ← This is the memo trace that the checkpoint path currently OMITS.
				//   Adding it here shows it would unblock Save Checkpoint.
				onEvent({
					type: 'log',
					runId: rid,
					at: '2026-04-10T00:00:00.025Z',
					level: 'info',
					nodeId,
					message: `[trace][memo.execute_decision] {"decision":"reuse","memoKey":"${MEMO_KEY}","reasonCode":"CHECKPOINT_TRUSTED_ARTIFACT"}`
				} as KnownRunEvent);
				onEvent({ type: 'node_finished', runId: rid, at: '2026-04-10T00:00:00.030Z', nodeId, status: 'succeeded', cached: true, execution_time_ms: 5 } as KnownRunEvent);
				onEvent({ type: 'run_finished', runId: rid, at: '2026-04-10T00:00:00.040Z', status: 'succeeded' } as KnownRunEvent);
			});
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');

		const binding = (get(graphStore).nodeBindings as any)?.[nodeId];

		// memo trace populated memoState.memoKey despite the non-hex lineage execKey.
		expect(binding?.memoState?.memoKey).toBe(MEMO_KEY);
		expect(/^[0-9a-f]{64}$/i.test(binding?.memoState?.memoKey ?? '')).toBe(true);

		// Save Checkpoint is now available.
		const result = graphStore.createCheckpoint(nodeId, 'fixed checkpoint');
		expect(result.ok).toBe(true);
		if (result.ok) {
			// fingerprintAtCreation comes from memoState, not the fallback execKey.
			expect(result.checkpoint.fingerprintAtCreation).toBe(MEMO_KEY);
		}
	});

	it('createCheckpoint replaces existing checkpoint for the same node', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-replace';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeSnapshot(graphId, nodeId, runId, true));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		const first = graphStore.createCheckpoint(nodeId, 'first');
		expect(first.ok).toBe(true);
		const second = graphStore.createCheckpoint(nodeId, 'second');
		expect(second.ok).toBe(true);
		expect((get(graphStore).checkpointRegistry as any)?.[nodeId]?.name).toBe('second');
	});

	it('renameCheckpoint and bulk checkpoint removals update registry', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-bulk';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeSnapshot(graphId, nodeId, runId, true));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		graphStore.createCheckpoint(nodeId, 'first');
		const renamed = graphStore.renameCheckpoint(nodeId, 'renamed');
		expect(renamed.ok).toBe(true);
		expect((get(graphStore).checkpointRegistry as any)?.[nodeId]?.name).toBe('renamed');

		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'n1',
					type: 'source',
					position: { x: 0, y: 0 },
					data: { kind: 'source', label: 'Source', sourceKind: 'text', params: {} }
				},
				{
					id: 'n2',
					type: 'source',
					position: { x: 20, y: 0 },
					data: { kind: 'source', label: 'Source 2', sourceKind: 'text', params: {} }
				}
			],
			edges: [],
			checkpointRegistry: {
				n1: {
					id: '00000000-0000-4000-8000-000000000001',
					name: 'valid',
					nodeId: 'n1',
					graphId,
					runId: 'r1',
					artifactId: 'a1',
					execKey: 'e1',
					fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					createdAt: '2026-04-10T00:00:00.000Z',
					staleness: 'valid'
				},
				n2: {
					id: '00000000-0000-4000-8000-000000000002',
					name: 'stale',
					nodeId: 'n2',
					graphId,
					runId: 'r2',
					artifactId: 'a2',
					execKey: 'e2',
					fingerprintAtCreation: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
					createdAt: '2026-04-10T00:00:00.000Z',
					staleness: 'stale'
				}
			} as any
		});
		const removedStale = graphStore.removeAllStaleCheckpoints();
		expect(removedStale.removed).toBe(1);
		expect((get(graphStore).checkpointRegistry as any)?.n2).toBeUndefined();
		expect((get(graphStore).checkpointRegistry as any)?.n1).toBeTruthy();

		const cleared = graphStore.clearAllCheckpoints();
		expect(cleared.removed).toBe(1);
		expect(get(graphStore).checkpointRegistry).toEqual({});
	});
});
