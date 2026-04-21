import { describe, expect, it } from 'vitest';
import { buildCheckpointPanelRows } from './buildCheckpointPanelRows';

describe('buildCheckpointPanelRows', () => {
	const graphNodes = [
		{ id: 'source_1', data: { label: 'DataSource', kind: 'source', params: {} } },
		{ id: 'comp_1', data: { label: 'ScorerComponent', kind: 'component', params: { componentRef: { componentId: 'cmp_score', revisionId: 'rev_1' } } } },
		{ id: 'comp_2', data: { label: 'LetterBuilder', kind: 'component', params: { componentRef: { componentId: 'cmp_letter', revisionId: 'rev_2' } } } }
	];

	const draftCache = {
		'cmp_score@rev_1': {
			__graphDraft: {
				nodes: [
					{ id: 'n_model', data: { label: 'ModelWorker', kind: 'model', params: {} } },
					{ id: 'n_letter', data: { label: 'LetterGen', kind: 'transform', params: {} } }
				],
				edges: [],
				checkpointRegistry: {
					n_model: { id: 'ck-1', name: 'model-ckpt', nodeId: 'n_model', graphId: 'g1', runId: 'r1', artifactId: 'art-1', execKey: 'exec-1', fingerprintAtCreation: 'a'.repeat(64), createdAt: '2026-04-10T00:00:00Z', staleness: 'valid' }
				}
			},
			__lastCommittedCheckpointRegistry: {}
		},
		'cmp_letter@rev_2': {
			__graphDraft: {
				nodes: [
					{ id: 'n_text', data: { label: 'TextWriter', kind: 'transform', params: {} } }
				],
				edges: [],
				checkpointRegistry: {}
			},
			__lastCommittedCheckpointRegistry: {}
		}
	};

	it('returns graph-level checkpoints with depth 0 and removable true', () => {
		const registry = {
			source_1: { id: 'ck-s1', name: 'source checkpoint', nodeId: 'source_1', graphId: 'g1', runId: 'r1', artifactId: 'art-s1', execKey: 'exec-s1', fingerprintAtCreation: 'b'.repeat(64), createdAt: '2026-04-10T00:00:00Z', staleness: 'valid' }
		};
		const rows = buildCheckpointPanelRows(registry, graphNodes, {}, null);
		expect(rows).toHaveLength(1);
		expect(rows[0].nodeId).toBe('source_1');
		expect(rows[0].nodeName).toBe('DataSource');
		expect(rows[0].depth).toBe(0);
		expect(rows[0].isPromoted).toBe(false);
		expect(rows[0].removable).toBe(true);
	});

	it('promotes cmp:-prefixed checkpoints with depth 1 and removable false', () => {
		const registry = {
			'cmp:comp_1:n_model': { id: 'ck-1', name: 'model-ckpt', nodeId: 'cmp:comp_1:n_model', graphId: 'g1', runId: 'r1', artifactId: 'art-1', execKey: 'exec-1', fingerprintAtCreation: 'a'.repeat(64), createdAt: '2026-04-10T00:00:00Z', staleness: 'valid' }
		};
		const rows = buildCheckpointPanelRows(registry, graphNodes, draftCache, null);
		expect(rows).toHaveLength(1);
		expect(rows[0].nodeId).toBe('cmp:comp_1:n_model');
		expect(rows[0].nodeName).toBe('ModelWorker');
		expect(rows[0].depth).toBe(1);
		expect(rows[0].isPromoted).toBe(true);
		expect(rows[0].componentPath).toBe('ScorerComponent');
		expect(rows[0].removable).toBe(false);
	});

	it('sorts graph-level before promoted, then by createdAt desc', () => {
		const registry = {
			source_1: { id: 'ck-s1', name: 'source-ckpt', nodeId: 'source_1', graphId: 'g1', runId: 'r1', artifactId: 'art-s1', execKey: 'exec-s1', fingerprintAtCreation: 'b'.repeat(64), createdAt: '2026-04-09T00:00:00Z', staleness: 'valid' },
			'cmp:comp_1:n_model': { id: 'ck-1', name: 'model-ckpt', nodeId: 'cmp:comp_1:n_model', graphId: 'g1', runId: 'r1', artifactId: 'art-1', execKey: 'exec-1', fingerprintAtCreation: 'a'.repeat(64), createdAt: '2026-04-10T00:00:00Z', staleness: 'valid' }
		};
		const rows = buildCheckpointPanelRows(registry, graphNodes, draftCache, null);
		expect(rows).toHaveLength(2);
		expect(rows[0].isPromoted).toBe(false);
		expect(rows[1].isPromoted).toBe(true);
	});

	it('resolves inner node label from component draft cache', () => {
		const registry = {
			'cmp:comp_1:n_letter': { id: 'ck-2', name: 'letter-ckpt', nodeId: 'cmp:comp_1:n_letter', graphId: 'g1', runId: 'r1', artifactId: 'art-2', execKey: 'exec-2', fingerprintAtCreation: 'c'.repeat(64), createdAt: '2026-04-10T00:00:00Z', staleness: 'stale' }
		};
		const rows = buildCheckpointPanelRows(registry, graphNodes, draftCache, null);
		expect(rows).toHaveLength(1);
		expect(rows[0].nodeName).toBe('LetterGen');
	});

	it('falls back to inner node ID when draft cache is unavailable', () => {
		const registry = {
			'cmp:comp_2:n_text': { id: 'ck-3', name: 'text-ckpt', nodeId: 'cmp:comp_2:n_text', graphId: 'g1', runId: 'r1', artifactId: 'art-3', execKey: 'exec-3', fingerprintAtCreation: 'd'.repeat(64), createdAt: '2026-04-10T00:00:00Z', staleness: 'valid' }
		};
		// comp_2 has no checkpoints in its draft, so inner node label falls back to n_text
		const rows = buildCheckpointPanelRows(registry, graphNodes, draftCache, null);
		expect(rows).toHaveLength(1);
		expect(rows[0].nodeName).toBe('TextWriter');
	});

	it('handles empty registry', () => {
		const rows = buildCheckpointPanelRows({}, graphNodes, {}, null);
		expect(rows).toHaveLength(0);
	});
});