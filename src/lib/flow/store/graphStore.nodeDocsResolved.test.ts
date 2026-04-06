import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';
import { __applyRunEventForTest, getNodeDocResolvedFromState, graphStore } from './graphStore';

describe('graphStore node docs resolved selector', () => {
	it('provides docs for all supported node kinds through BaseNode inheritance path', () => {
		graphStore.hardResetGraph();
		const nodeKinds: Array<'source' | 'transform' | 'model' | 'tool' | 'component'> = [
			'source',
			'transform',
			'model',
			'tool',
			'component'
		];
		const nodeIds = nodeKinds.map((kind, index) => graphStore.addNode(kind as any, { x: index * 220, y: 0 }));
		const state = get(graphStore as any);
		for (const nodeId of nodeIds) {
			const resolved = getNodeDocResolvedFromState(state as any, nodeId);
			expect(resolved).not.toBeNull();
		}
	});

	it('includes runtime handles from node declarations and connected edges', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const modelId = graphStore.addNode('model', { x: 220, y: 0 });
		graphStore.addEdge({
			id: 'e_doc_work',
			source: sourceId,
			target: modelId,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { mode: 'work' }
		} as any);
		const state = get(graphStore as any);
		const resolved = getNodeDocResolvedFromState(state as any, modelId);
		expect(resolved).not.toBeNull();
		expect(resolved?.planes?.data?.ports?.some((port) => port.direction === 'in' && port.handle === 'in')).toBe(
			true
		);
	});

	it('includes instance-level deterministic context notes', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		graphStore.setSourceKind(sourceId as any, 'api');
		graphStore.updateNodeConfig(sourceId, { params: { url: 'https://example.com/jobs', method: 'GET' } });
		const state = get(graphStore as any);
		const resolved = getNodeDocResolvedFromState(state as any, sourceId);
		const paramNotes = resolved?.planes?.param?.notes ?? [];
		expect(paramNotes.some((note) => note.includes('source_kind=api'))).toBe(true);
	});

	it('reflects blocked reason from scheduler snapshot in control section', () => {
		graphStore.hardResetGraph();
		const modelId = graphStore.addNode('model', { x: 0, y: 0 });
		const base = get(graphStore as any);
		const next = __applyRunEventForTest(
			base as any,
			{
				type: 'scheduler_snapshot',
				runId: 'run_docs_1',
				at: '2026-04-05T20:00:00.000Z',
				readyCount: 0,
				inflightCount: 0,
				pendingQueueDepth: 2,
				runnableNodeCount: 0,
				stalled: false,
				perNode: [
					{
						nodeId: modelId,
						readyWork: false,
						inflight: 0,
						pendingInputCount: 2,
						lastBlockedReasonCode: 'WAITING_REQUIRED_INPUT'
					}
				]
			} as any,
			'run_docs_1'
		);
		const resolved = getNodeDocResolvedFromState(next as any, modelId);
		expect(resolved?.planes.control.notes?.some((note) => note.includes('WAITING_REQUIRED_INPUT'))).toBe(true);
	});

	it('returns runtime-only doc envelope when no base docs exist', () => {
		graphStore.hardResetGraph();
		const modelId = graphStore.addNode('model', { x: 0, y: 0 });
		const state = get(graphStore as any);
		const cloned = structuredClone(state);
		const targetNode = cloned.nodes.find((node: any) => node.id === modelId);
		targetNode.data.kind = 'unknown_kind';
		const resolved = getNodeDocResolvedFromState(cloned as any, modelId);
		expect(resolved?.source).toBe('runtime_only');
	});

	it('applies node-level doc override summary and disabled flag', () => {
		graphStore.hardResetGraph();
		const modelId = graphStore.addNode('model', { x: 0, y: 0 });
		const state = structuredClone(get(graphStore as any));
		const targetNode = state.nodes.find((node: any) => node.id === modelId);
		targetNode.data.meta = {
			...(targetNode.data.meta ?? {}),
			nodeDoc: {
				summary: 'Instance-level docs summary',
				disabled: true
			}
		};
		const resolved = getNodeDocResolvedFromState(state as any, modelId);
		expect(resolved?.summary).toBe('Instance-level docs summary');
		expect(resolved?.disabled).toBe(true);
	});

	it('persists and clears generated AI explanation on node meta', () => {
		graphStore.hardResetGraph();
		const modelId = graphStore.addNode('model', { x: 0, y: 0 });
		const save = graphStore.setNodeDocGeneratedExplanation(modelId, {
			summary: 'AI generated summary',
			settings_explained: ['model=glm-4.7-flash:latest'],
			context_notes: ['pending_input_count=0'],
			generated_at: '2026-04-06T00:00:00.000Z',
			signature_key: 'sig_persist_ai_1'
		});
		expect(save.ok).toBe(true);
		const afterSave = get(graphStore as any);
		const resolvedAfterSave = getNodeDocResolvedFromState(afterSave as any, modelId);
		expect(resolvedAfterSave?.generated?.signature_key).toBe('sig_persist_ai_1');
		expect(resolvedAfterSave?.generated?.summary).toBe('AI generated summary');
		const clear = graphStore.clearNodeDocGeneratedExplanation(modelId);
		expect(clear.ok).toBe(true);
		const afterClear = get(graphStore as any);
		const resolvedAfterClear = getNodeDocResolvedFromState(afterClear as any, modelId);
		expect(resolvedAfterClear?.generated ?? null).toBeNull();
	});

	it('does not mutate graph state while resolving docs', () => {
		graphStore.hardResetGraph();
		const modelId = graphStore.addNode('model', { x: 0, y: 0 });
		const before = get(graphStore as any);
		const beforeJson = JSON.stringify(before);
		getNodeDocResolvedFromState(before as any, modelId);
		const after = get(graphStore as any);
		expect(JSON.stringify(after)).toBe(beforeJson);
	});
});
