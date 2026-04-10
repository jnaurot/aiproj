import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore, __stripToDTOForTest } from './graphStore';
import { buildPersistableGraphStrict } from './graphStore.persistence';
import { clearGraphDraft, emptyGraph, loadGraphFromLocalStorage, saveGraphToLocalStorage } from './persist';

function installWindowLocalStorageForTest(): void {
	const storage = new Map<string, string>();
	(globalThis as any).window = {
		localStorage: {
			getItem: (key: string) => (storage.has(key) ? String(storage.get(key)) : null),
			setItem: (key: string, value: string) => {
				storage.set(String(key), String(value));
			},
			removeItem: (key: string) => {
				storage.delete(String(key));
			}
		}
	};
}

function sampleCheckpointRegistry() {
	return {
		n1: {
			id: 'f5f00d93-c51d-4a39-b9d9-85f731fb86d7',
			name: 'ck-1',
			nodeId: 'n1',
			graphId: 'graph-checkpoint',
			runId: 'run-1',
			artifactId: 'art-1',
			execKey: 'exec-1',
			fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
			createdAt: '2026-04-10T00:00:00.000Z',
			staleness: 'unknown'
		}
	};
}

describe('graphStore checkpoint registry persistence', () => {
	it('checkpoint registry initializes empty on hard reset', () => {
		graphStore.hardResetGraph();
		expect(get(graphStore).checkpointRegistry).toEqual({});
	});

	it('checkpoint registry survives local persist and reload', () => {
		installWindowLocalStorageForTest();
		clearGraphDraft();
		const dto = __stripToDTOForTest([], [], 'graph-checkpoint', sampleCheckpointRegistry() as any);
		saveGraphToLocalStorage(dto);
		const loaded = loadGraphFromLocalStorage(emptyGraph);
		expect((loaded as any).checkpointRegistry).toEqual(sampleCheckpointRegistry());
	});

	it('legacy graph without checkpointRegistry loads with empty registry', () => {
		installWindowLocalStorageForTest();
		const legacy = {
			schemaVersion: 1,
			updatedAt: '2026-04-10T00:00:00.000Z',
			graphId: 'graph-legacy',
			graph: {
				version: 1,
				nodes: [],
				edges: []
			}
		};
		window.localStorage.setItem('flow:graph:v1', JSON.stringify(legacy));
		const loaded = loadGraphFromLocalStorage(emptyGraph);
		expect((loaded as any).checkpointRegistry).toEqual({});
	});

	it('buildPersistableGraphStrict includes checkpointRegistry in output DTO', () => {
		const result = buildPersistableGraphStrict([], [], 'graph-checkpoint', sampleCheckpointRegistry() as any);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect((result.graph as any).checkpointRegistry).toEqual(sampleCheckpointRegistry());
		}
	});
});

describe('graphStore legacy pin migration to checkpoint registry', () => {
	it('migrates legacy freeze pin to checkpoint and strips legacy fields on load', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'n_legacy',
						type: 'model',
						position: { x: 0, y: 0 },
						data: {
							kind: 'model',
							label: 'Legacy',
							params: {},
							status: 'idle',
							meta: {
								updatedAt: '2026-04-10T00:00:00.000Z',
								freeze: { enabled: true, mode: 'sticky' },
								freezeLineage: { artifactId: 'art-legacy', execKey: 'exec-legacy' }
							}
						}
					}
				],
				edges: []
			},
			'graph_migrate'
		);
		expect(loaded.ok).toBe(true);
		const state = get(graphStore);
		const migrated = (state.checkpointRegistry as any)?.n_legacy;
		expect(migrated).toBeTruthy();
		expect(migrated?.artifactId).toBe('art-legacy');
		expect(migrated?.execKey).toBe('exec-legacy');
		expect(migrated?.graphId).toBe('graph_migrate');
		expect(migrated?.staleness).toBe('unknown');
		const node = state.nodes.find((n) => n.id === 'n_legacy');
		expect((node?.data as any)?.meta?.freeze).toBeUndefined();
		expect((node?.data as any)?.meta?.freezeLineage).toBeUndefined();
	});

	it('skips migration when checkpoint already exists and still strips legacy fields', () => {
		graphStore.hardResetGraph();
		const existing = sampleCheckpointRegistry() as any;
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'n1',
						type: 'model',
						position: { x: 0, y: 0 },
						data: {
							kind: 'model',
							label: 'Legacy',
							params: {},
							status: 'idle',
							meta: {
								freeze: { enabled: true, mode: 'per_run' },
								freezeLineage: { artifactId: 'art-new', execKey: 'exec-new' }
							}
						}
					}
				],
				edges: [],
				checkpointRegistry: existing
			},
			'graph_skip'
		);
		expect(loaded.ok).toBe(true);
		const state = get(graphStore);
		expect((state.checkpointRegistry as any)?.n1?.artifactId).toBe('art-1');
		expect((state.checkpointRegistry as any)?.n1?.execKey).toBe('exec-1');
		const node = state.nodes.find((n) => n.id === 'n1');
		expect((node?.data as any)?.meta?.freeze).toBeUndefined();
		expect((node?.data as any)?.meta?.freezeLineage).toBeUndefined();
	});

	it('strips legacy fields without creating checkpoint when lineage is missing', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'n_missing',
						type: 'model',
						position: { x: 0, y: 0 },
						data: {
							kind: 'model',
							label: 'Legacy Missing',
							params: {},
							status: 'idle',
							meta: {
								freeze: { enabled: true, mode: 'sticky' }
							}
						}
					}
				],
				edges: []
			},
			'graph_missing_lineage'
		);
		expect(loaded.ok).toBe(true);
		const state = get(graphStore);
		expect((state.checkpointRegistry as any)?.n_missing).toBeUndefined();
		const node = state.nodes.find((n) => n.id === 'n_missing');
		expect((node?.data as any)?.meta?.freeze).toBeUndefined();
		expect((node?.data as any)?.meta?.freezeLineage).toBeUndefined();
	});
});
