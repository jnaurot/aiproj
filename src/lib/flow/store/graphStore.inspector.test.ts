import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

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
		expect((state.inspector.draftParams as any).delimiter).toBe(';');
		expect(state.inspector.dirty).toBe(true);
	});
});
