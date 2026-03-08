import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';
import { __applyRunEventForTest, __normalizeBindingForTest, type GraphState } from './graphStore';
import { getHeaderCachePill } from '$lib/flow/components/inspectorCachePill';
import { displayStatusFromBinding } from './runScope';

function setupSourceNode(): string {
	graphStore.hardResetGraph();
	const nodeId = graphStore.addNode('source', { x: 10, y: 10 });
	graphStore.selectNode(nodeId);
	return nodeId;
}

function sourceNodeParams(nodeId: string): Record<string, any> {
	const state = get(graphStore);
	const node = state.nodes.find((n) => n.id === nodeId);
	return ((node?.data as any)?.params ?? {}) as Record<string, any>;
}

describe('graphStore snapshot scoped commit', () => {
	it('source output port change syncs params.output.mode and marks node stale', () => {
		const nodeId = setupSourceNode();
		const initial = sourceNodeParams(nodeId);
		expect(initial.output?.mode).toBeTruthy();

		const result = graphStore.updateNodeConfig(nodeId, { ports: { out: 'json' } });
		expect(result.ok).toBe(true);

		const state = get(graphStore);
		const params = sourceNodeParams(nodeId);
		expect(params.output?.mode).toBe('json');
		expect(state.nodeBindings[nodeId]?.staleReason).toBe('PORTS_CHANGED');
		expect(displayStatusFromBinding(state.nodeBindings[nodeId])).toBe('stale');
	});

	it('selecting_previous_upload_commits_snapshot_without_dirty_state', async () => {
		const nodeId = setupSourceNode();
		const snapshotId = 'a'.repeat(64);

		expect(get(graphStore).inspector.dirty).toBe(false);

		await graphStore.commitSnapshotSelection({
			snapshotId,
			snapshotMetadata: {
				snapshotId,
				originalFilename: 'README.md',
				byteSize: 123
			},
			recentSnapshotIds: [snapshotId],
			recentSnapshots: [{ id: snapshotId, filename: 'README.md', size: 123 }]
		});

		const state = get(graphStore);
		expect(state.inspector.dirty).toBe(false);
		expect(sourceNodeParams(nodeId).snapshotId).toBe(snapshotId);
		expect((state.inspector.draftParams as any).snapshotId).toBe(snapshotId);
	});

	it('node_finished failed stores structured missing-column error details', () => {
		const nodeId = setupSourceNode();
		const state = get(graphStore) as GraphState;
		const next = __applyRunEventForTest(
			state,
			{
				type: 'node_finished',
				runId: 'run_missing_column',
				at: '2026-03-03T00:00:00Z',
				nodeId,
				status: 'failed',
				error: 'Transform payload schema mismatch: dedupe references missing columns',
				errorCode: 'MISSING_COLUMN',
				errorDetails: {
					op: 'dedupe',
					paramPath: 'by',
					missingColumns: ['missing'],
					availableColumns: ['text', 'other'],
					availableColumnsSource: 'schema'
				}
			} as any,
			'run_missing_column'
		);
		expect(next.nodeOutputs[nodeId]?.lastError?.errorCode).toBe('MISSING_COLUMN');
		expect(next.nodeOutputs[nodeId]?.lastError?.paramPath).toBe('by');
		expect(next.nodeOutputs[nodeId]?.lastError?.missingColumns).toEqual(['missing']);
		expect(next.nodeOutputs[nodeId]?.lastError?.availableColumns).toEqual(['text', 'other']);
	});

	it('selecting_previous_upload_does_not_accept_unrelated_drafts', async () => {
		const nodeId = setupSourceNode();
		const snapshotId = 'b'.repeat(64);
		const beforeCommittedDelimiter = sourceNodeParams(nodeId).delimiter;

		graphStore.patchInspectorDraft({ delimiter: ';' });
		let state = get(graphStore);
		expect(state.inspector.dirty).toBe(true);
		expect((state.inspector.draftParams as any).delimiter).toBe(';');

		await graphStore.commitSnapshotSelection({
			snapshotId,
			snapshotMetadata: {
				snapshotId,
				originalFilename: 'notes.txt',
				byteSize: 77
			},
			recentSnapshotIds: [snapshotId],
			recentSnapshots: [{ id: snapshotId, filename: 'notes.txt', size: 77 }]
		});

		state = get(graphStore);
		const committed = sourceNodeParams(nodeId);
		expect(committed.snapshotId).toBe(snapshotId);
		expect(committed.delimiter).toBe(beforeCommittedDelimiter);
		expect((state.inspector.draftParams as any).delimiter).toBe(beforeCommittedDelimiter);
		expect(state.inspector.dirty).toBe(false);
	});

	it('upload-style invalidation clears cache ui and run decisions repopulate cache pill semantics', async () => {
		const nodeId = setupSourceNode();
		const sidA = 'c'.repeat(64);
		const sidB = 'd'.repeat(64);
		let resolveCalls = 0;
		const fetchSpy = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			if (!url.includes('/api/runs/resolve/source')) {
				return new Response(JSON.stringify({}), { status: 200 });
			}
			resolveCalls += 1;
			if (resolveCalls === 1) {
				return new Response(
					JSON.stringify({
						graphId: 'graph_test',
						nodeId,
						execKey: 'exec_c',
						artifactId: 'artifact_c',
						cacheHit: true,
						artifact: { artifactId: 'artifact_c', mimeType: 'text/plain', portType: 'text' }
					}),
					{ status: 200 }
				);
			}
			return new Response(
				JSON.stringify({
					graphId: 'graph_test',
					nodeId,
					execKey: 'exec_d',
					artifactId: null,
					cacheHit: false
				}),
				{ status: 200 }
			);
		};

		try {
			await graphStore.commitSnapshotSelection({
				snapshotId: sidA,
				snapshotMetadata: { snapshotId: sidA, originalFilename: 'a.txt', byteSize: 10 },
				recentSnapshotIds: [sidA],
				recentSnapshots: [{ id: sidA, filename: 'a.txt', size: 10 }]
			});

			let state = get(graphStore);
			let status = displayStatusFromBinding(state.nodeBindings[nodeId]);
			let pill = getHeaderCachePill(state.nodeOutputs[nodeId], state.nodeBindings[nodeId], status);
			expect(pill?.label).toBe('cached');

			await graphStore.commitSnapshotSelection({
				snapshotId: sidB,
				snapshotMetadata: { snapshotId: sidB, originalFilename: 'b.txt', byteSize: 11 },
				recentSnapshotIds: [sidB, sidA],
				recentSnapshots: [
					{ id: sidB, filename: 'b.txt', size: 11 },
					{ id: sidA, filename: 'a.txt', size: 10 }
				]
			});

			state = get(graphStore);
			status = displayStatusFromBinding(state.nodeBindings[nodeId]);
			expect(status).toBe('stale');
			expect(state.nodeOutputs[nodeId]?.cacheDecision).toBeUndefined();
			expect(state.nodeOutputs[nodeId]?.cached).toBe(false);
			expect(state.inspector.dirty).toBe(false);
			pill = getHeaderCachePill(state.nodeOutputs[nodeId], state.nodeBindings[nodeId], status);
			expect(pill).toBeNull();

			const baseState = get(graphStore) as GraphState;
			const runId = 'run-cache-pill';
			const seed: GraphState = {
				...baseState,
				activeRunId: runId,
				nodeBindings: {
					...baseState.nodeBindings,
					[nodeId]: __normalizeBindingForTest(
						{ ...baseState.nodeBindings[nodeId], status: 'succeeded_up_to_date', isUpToDate: true },
						nodeId
					)
				}
			};
			const hitDecisionState = __applyRunEventForTest(
				seed,
				{
					type: 'cache_decision',
					runId,
					at: '2026-03-02T00:00:00Z',
					nodeId,
					nodeKind: 'source',
					decision: 'cache_hit',
					execKey: 'ek_hit',
					artifactId: 'art_hit'
				},
				runId
			);
			const hitState = __applyRunEventForTest(
				hitDecisionState,
				{
					type: 'node_finished',
					runId,
					at: '2026-03-02T00:00:00Z',
					nodeId,
					status: 'succeeded'
				},
				runId
			);
			expect(hitState.nodeOutputs[nodeId]?.cacheDecision).toBe('cache_hit');
			expect(
				getHeaderCachePill(
					hitState.nodeOutputs[nodeId],
					hitState.nodeBindings[nodeId],
					displayStatusFromBinding(hitState.nodeBindings[nodeId])
				)?.label
			).toBe('cached');

			const missDecisionState = __applyRunEventForTest(
				seed,
				{
					type: 'cache_decision',
					runId,
					at: '2026-03-02T00:00:01Z',
					nodeId,
					nodeKind: 'source',
					decision: 'cache_miss',
					execKey: 'ek_miss'
				},
				runId
			);
			const missState = __applyRunEventForTest(
				missDecisionState,
				{
					type: 'node_finished',
					runId,
					at: '2026-03-02T00:00:01Z',
					nodeId,
					status: 'succeeded'
				},
				runId
			);
			expect(missState.nodeOutputs[nodeId]?.cacheDecision).toBe('cache_miss');
			expect(missState.nodeOutputs[nodeId]?.cached).toBe(false);
			expect(
				getHeaderCachePill(
					missState.nodeOutputs[nodeId],
					missState.nodeBindings[nodeId],
					displayStatusFromBinding(missState.nodeBindings[nodeId])
				)
			).toBeNull();

			const mismatchDecisionState = __applyRunEventForTest(
				seed,
				{
					type: 'cache_decision',
					runId,
					at: '2026-03-02T00:00:02Z',
					nodeId,
					nodeKind: 'source',
					decision: 'cache_hit_contract_mismatch',
					execKey: 'ek_mm',
					expectedContractFingerprint: 'exp',
					actualContractFingerprint: 'act'
				} as any,
				runId
			);
			const mismatchOutputState = __applyRunEventForTest(
				mismatchDecisionState,
				{
					type: 'node_output',
					runId,
					at: '2026-03-02T00:00:02Z',
					nodeId,
					artifactId: 'art_mm',
					preview: 'mismatch-output',
					mimeType: 'text/plain',
					portType: 'text'
				} as any,
				runId
			);
			const mismatchState = __applyRunEventForTest(
				mismatchOutputState,
				{
					type: 'node_finished',
					runId,
					at: '2026-03-02T00:00:02Z',
					nodeId,
					status: 'succeeded'
				},
				runId
			);
			expect(mismatchState.nodeOutputs[nodeId]?.cacheDecision).toBe('cache_miss');
			expect(mismatchState.nodeOutputs[nodeId]?.cached).toBe(false);
			expect(
				getHeaderCachePill(
					mismatchState.nodeOutputs[nodeId],
					mismatchState.nodeBindings[nodeId],
					displayStatusFromBinding(mismatchState.nodeBindings[nodeId])
				)?.label
			).toBeUndefined();
		} finally {
			(globalThis as any).fetch = fetchSpy;
		}
	});

	it('source kind switch file->database is semantic reset (stale + cache cleared + no header cache pill)', async () => {
		const nodeId = setupSourceNode();
		const sid = 'e'.repeat(64);
		const fetchSpy = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			if (!url.includes('/api/runs/resolve/source')) {
				return new Response(JSON.stringify({}), { status: 200 });
			}
			return new Response(
				JSON.stringify({
					graphId: 'graph_test',
					nodeId,
					execKey: 'exec_e',
					artifactId: 'artifact_e',
					cacheHit: true,
					artifact: { artifactId: 'artifact_e', mimeType: 'text/plain', portType: 'text' }
				}),
				{ status: 200 }
			);
		};

		try {
			await graphStore.commitSnapshotSelection({
				snapshotId: sid,
				snapshotMetadata: { snapshotId: sid, originalFilename: 'before.txt', byteSize: 12 },
				recentSnapshotIds: [sid],
				recentSnapshots: [{ id: sid, filename: 'before.txt', size: 12 }]
			});

			let state = get(graphStore);
			expect(displayStatusFromBinding(state.nodeBindings[nodeId])).toBe('succeeded');
			expect(
				getHeaderCachePill(
					state.nodeOutputs[nodeId],
					state.nodeBindings[nodeId],
					displayStatusFromBinding(state.nodeBindings[nodeId])
				)?.label
			).toBe('cached');

			const result = graphStore.setSourceKind(nodeId, 'database');
			expect(result.ok).toBe(true);

			state = get(graphStore);
			const node = state.nodes.find((n) => n.id === nodeId)!;
			expect((node.data as any).sourceKind).toBe('database');
			expect(displayStatusFromBinding(state.nodeBindings[nodeId])).toBe('stale');
			expect(state.nodeBindings[nodeId].isUpToDate).toBe(false);
			expect(state.nodeBindings[nodeId].staleReason).toBe('KIND_CHANGED');
			expect(state.nodeBindings[nodeId].current?.artifactId ?? null).toBeNull();
			expect(state.nodeOutputs[nodeId]?.cacheDecision).toBeUndefined();
			expect(state.nodeOutputs[nodeId]?.cached).toBe(false);
			expect(
				getHeaderCachePill(
					state.nodeOutputs[nodeId],
					state.nodeBindings[nodeId],
					displayStatusFromBinding(state.nodeBindings[nodeId])
				)
			).toBeNull();
		} finally {
			(globalThis as any).fetch = fetchSpy;
		}
	});

	it('kind switch lifecycle: stale hides cache pill, next run decision controls pill appearance', () => {
		const nodeId = 'n_source';
		const runId = 'run-kind-switch';
		const seed: GraphState = {
			graphId: 'graph-kind-switch',
			nodes: [{ id: nodeId }, { id: 'n_downstream' }] as any,
			edges: [{ id: 'e1', source: nodeId, target: 'n_downstream' }] as any,
			selectedNodeId: nodeId,
			inspector: { nodeId, draftParams: {}, dirty: false, uiByNodeId: {} },
			logs: [],
			runStatus: 'running',
			lastRunStatus: 'succeeded',
			freshness: 'stale',
			staleNodeCount: 1,
			activeRunMode: 'from_start',
			activeRunFrom: null,
			activeRunNodeSet: new Set([nodeId, 'n_downstream']),
			nodeOutputs: {
				[nodeId]: { cacheDecision: undefined, cached: false }
			},
			nodeBindings: {
				[nodeId]: __normalizeBindingForTest(
					{
						status: 'stale',
						isUpToDate: false,
						staleReason: 'KIND_CHANGED',
						current: { execKey: null, artifactId: null },
						last: { execKey: 'ek_old', artifactId: 'art_old' }
					},
					nodeId
				)
			} as any,
			activeRunId: runId
		};

		expect(
			getHeaderCachePill(
				seed.nodeOutputs[nodeId],
				seed.nodeBindings[nodeId],
				displayStatusFromBinding(seed.nodeBindings[nodeId])
			)
		).toBeNull();

		const hit = __applyRunEventForTest(
			__applyRunEventForTest(
				seed,
				{
					type: 'cache_decision',
					runId,
					at: '2026-03-02T00:10:00Z',
					nodeId,
					nodeKind: 'source',
					decision: 'cache_hit',
					execKey: 'ek_new',
					artifactId: 'art_new'
				},
				runId
			),
			{
				type: 'node_finished',
				runId,
				at: '2026-03-02T00:10:01Z',
				nodeId,
				status: 'succeeded'
			},
			runId
		);
		expect(
			getHeaderCachePill(
				hit.nodeOutputs[nodeId],
				hit.nodeBindings[nodeId],
				displayStatusFromBinding(hit.nodeBindings[nodeId])
			)?.label
		).toBe('cached');

		const miss = __applyRunEventForTest(
			__applyRunEventForTest(
				seed,
				{
					type: 'cache_decision',
					runId,
					at: '2026-03-02T00:10:02Z',
					nodeId,
					nodeKind: 'source',
					decision: 'cache_miss',
					execKey: 'ek_new'
				},
				runId
			),
			{
				type: 'node_finished',
				runId,
				at: '2026-03-02T00:10:03Z',
				nodeId,
				status: 'succeeded'
			},
			runId
		);
		expect(
			getHeaderCachePill(
				miss.nodeOutputs[nodeId],
				miss.nodeBindings[nodeId],
				displayStatusFromBinding(miss.nodeBindings[nodeId])
			)
		).toBeNull();
	});

});
