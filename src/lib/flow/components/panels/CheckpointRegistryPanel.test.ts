import { describe, expect, it } from 'vitest';
import { render } from 'svelte/server';

import CheckpointRegistryPanel from './CheckpointRegistryPanel.svelte';

function row(nodeId: string, staleness: 'valid' | 'stale' | 'artifact_missing' | 'unknown') {
	return {
		nodeId,
		nodeName: `Node ${nodeId}`,
		checkpoint: {
			id: '00000000-0000-4000-8000-000000000001',
			name: `ck-${nodeId}`,
			nodeId,
			graphId: 'g',
			runId: 'r',
			artifactId: `art-${nodeId}`,
			execKey: `exec-${nodeId}`,
			fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
			createdAt: '2026-04-10T00:00:00.000Z',
			staleness
		}
	};
}

describe('CheckpointRegistryPanel', () => {
	it('renders empty state when there are no checkpoints', () => {
		const { body } = render(CheckpointRegistryPanel as any, { props: { rows: [] } });
		expect(body).toContain('No checkpoints. Run a node and save its output as a checkpoint.');
	});

	it('renders checkpoint row with stale badge class', () => {
		const { body } = render(CheckpointRegistryPanel as any, { props: { rows: [row('n1', 'stale')] } });
		expect(body).toContain('Node n1');
		expect(body).toContain('badge-stale');
		expect(body).toContain('>stale<');
	});

	it('disables remove-all-stale when no stale entries exist', () => {
		const { body } = render(CheckpointRegistryPanel as any, { props: { rows: [row('n1', 'valid')] } });
		expect(body).toContain('Remove all stale');
		expect(body).toContain('disabled');
	});
});

