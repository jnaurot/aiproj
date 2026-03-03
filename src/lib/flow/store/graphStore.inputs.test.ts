import { describe, expect, it } from 'vitest';
import type { Edge, Node } from '@xyflow/svelte';

import type { PipelineEdgeData, PipelineNodeData } from '$lib/flow/types';
import {
	__normalizeBindingForTest,
	resolveNodeInputsFromState,
	type GraphState
} from './graphStore';

function makeNode(
	id: string,
	kind: PipelineNodeData['kind'],
	ports: { in?: PipelineNodeData['ports']['in']; out?: PipelineNodeData['ports']['out'] }
): Node<PipelineNodeData> {
	return {
		id,
		type: kind,
		position: { x: 0, y: 0 },
		data: {
			kind,
			label: id,
			params: {},
			status: 'idle',
			ports
		} as any
	};
}

function makeState(args?: {
	edges?: Edge<PipelineEdgeData>[];
	activeRunId?: string | null;
	upstreamBinding?: Record<string, any>;
}): GraphState {
	const upstreamId = 'n_up';
	const downstreamId = 'n_down';
	const nodes: Node<PipelineNodeData>[] = [
		makeNode(upstreamId, 'source', { in: null, out: 'table' }),
		makeNode(downstreamId, 'transform', { in: 'table', out: 'table' })
	];
	const edges =
		args?.edges ??
		([
			{
				id: 'e1',
				source: upstreamId,
				sourceHandle: 'out',
				target: downstreamId,
				targetHandle: 'in',
				data: { exec: 'idle' }
			}
		] as Edge<PipelineEdgeData>[]);
	const upstreamBinding = __normalizeBindingForTest(
		{
			status: 'idle',
			current: { execKey: null, artifactId: null },
			last: { execKey: null, artifactId: null },
			currentRunId: null,
			isUpToDate: false,
			cacheValid: false,
			staleReason: null,
			...(args?.upstreamBinding ?? {})
		},
		upstreamId
	);
	const downstreamBinding = __normalizeBindingForTest(undefined, downstreamId);
	return {
		graphId: 'graph_test',
		nodes: nodes as any,
		edges: edges as any,
		selectedNodeId: downstreamId,
		inspector: { nodeId: null, draftParams: {}, dirty: false, uiByNodeId: {} },
		logs: [],
		runStatus: 'idle',
		lastRunStatus: 'never_run',
		freshness: 'never_run' as any,
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(),
		nodeOutputs: {
			[upstreamId]: {
				mimeType: 'text/csv'
			}
		},
		nodeBindings: {
			[upstreamId]: upstreamBinding,
			[downstreamId]: downstreamBinding
		},
		activeRunId: args?.activeRunId ?? null
	};
}

describe('resolveNodeInputsFromState', () => {
	it('prefers active-run produced upstream artifact', () => {
		const state = makeState({
			activeRunId: 'run_active',
			upstreamBinding: {
				status: 'running',
				currentRunId: 'run_active',
				current: { execKey: 'ek_active', artifactId: 'art_active' },
				last: { execKey: 'ek_last', artifactId: 'art_last' }
			}
		});

		const out = resolveNodeInputsFromState(state, 'n_down');
		expect(out).toHaveLength(1);
		expect(out[0].status).toBe('resolved');
		expect(out[0].artifactId).toBe('art_active');
		expect(out[0].artifactSource).toBe('active_run');
	});

	it('uses bound artifact when no active-run artifact is available', () => {
		const state = makeState({
			activeRunId: 'run_other',
			upstreamBinding: {
				status: 'stale',
				currentRunId: 'run_prev',
				current: { execKey: 'ek_bound', artifactId: 'art_bound' },
				last: { execKey: 'ek_last', artifactId: 'art_last' }
			}
		});

		const out = resolveNodeInputsFromState(state, 'n_down');
		expect(out).toHaveLength(1);
		expect(out[0].status).toBe('resolved');
		expect(out[0].artifactId).toBe('art_bound');
		expect(out[0].artifactSource).toBe('bound');
	});

	it('returns DISCONNECTED when no upstream edge exists for input port', () => {
		const state = makeState({ edges: [] });
		const out = resolveNodeInputsFromState(state, 'n_down');
		expect(out).toHaveLength(1);
		expect(out[0].status).toBe('missing');
		expect(out[0].reason).toBe('DISCONNECTED');
	});

	it('returns UPSTREAM_FAILED when upstream failed and has no artifact', () => {
		const state = makeState({
			upstreamBinding: {
				status: 'failed',
				currentRunId: null,
				current: { execKey: null, artifactId: null },
				last: { execKey: null, artifactId: null }
			}
		});
		const out = resolveNodeInputsFromState(state, 'n_down');
		expect(out).toHaveLength(1);
		expect(out[0].status).toBe('missing');
		expect(out[0].reason).toBe('UPSTREAM_FAILED');
	});

	it('returns UPSTREAM_NO_ARTIFACT when upstream has not produced an artifact yet', () => {
		const state = makeState({
			upstreamBinding: {
				status: 'running',
				currentRunId: 'run_current',
				current: { execKey: null, artifactId: null },
				last: { execKey: null, artifactId: null }
			}
		});
		const out = resolveNodeInputsFromState(state, 'n_down');
		expect(out).toHaveLength(1);
		expect(out[0].status).toBe('missing');
		expect(out[0].reason).toBe('UPSTREAM_NO_ARTIFACT');
	});
});

