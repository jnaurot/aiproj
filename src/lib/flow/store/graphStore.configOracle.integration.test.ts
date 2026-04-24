import { describe, expect, it } from 'vitest';

import { createSchemaPlaneManager } from './graphStore.schemaPlane';

function makeManager(state: any) {
	return createSchemaPlaneManager({
		getState: () => state as any
	});
}

describe('graphStore schema configuration oracle integration', () => {
	it('returns availableColumns from incoming table edge schema', () => {
		const state = {
			nodes: [{ id: 'select_node', data: { kind: 'transform', transformKind: 'select' } }],
			edges: [{ id: 'e_in', source: 'src', target: 'select_node', targetHandle: 'in' }],
			schemaPlane: {
				nodeSchemas: {},
				edgeSchemas: {
					e_in: {
						mode: 'table',
						columns: [
							{ name: 'id', type: 'number', nullable: false, properties: {} },
							{ name: 'text', type: 'string', nullable: true, properties: {} }
						]
					}
				}
			}
		};
		const manager = makeManager(state);
		const hints = manager.getConfigurationHints('select_node');
		expect(hints.availableColumns).toEqual(['id', 'text']);
	});

	it('derives join.availableKeys from two incoming table schemas', () => {
		const state = {
			nodes: [{ id: 'join_node', data: { kind: 'transform', transformKind: 'join' } }],
			edges: [
				{ id: 'e_left', source: 'left', target: 'join_node', targetHandle: 'left' },
				{ id: 'e_right', source: 'right', target: 'join_node', targetHandle: 'right' }
			],
			schemaPlane: {
				nodeSchemas: {},
				edgeSchemas: {
					e_left: {
						mode: 'table',
						columns: [
							{ name: 'id', type: 'number', nullable: false, properties: {} },
							{ name: 'left_only', type: 'string', nullable: true, properties: {} }
						]
					},
					e_right: {
						mode: 'table',
						columns: [
							{ name: 'id', type: 'number', nullable: false, properties: {} },
							{ name: 'right_only', type: 'string', nullable: true, properties: {} }
						]
					}
				}
			}
		};
		const manager = makeManager(state);
		const hints = manager.getConfigurationHints('join_node');
		expect(hints.suggestions?.['join.availableKeys']).toEqual(['id']);
	});

	it('derives join.availableKeys from nodeId-qualified clauses on same handle', () => {
		const state = {
			nodes: [
				{
					id: 'join_node',
					data: {
						kind: 'transform',
						transformKind: 'join',
						params: {
							op: 'join',
							join: {
								clauses: [
									{
										leftNodeId: 'left_node',
										leftCol: 'id',
										rightNodeId: 'right_node',
										rightCol: 'id',
										how: 'inner'
									}
								]
							}
						}
					}
				}
			],
			edges: [
				{ id: 'e_left', source: 'left_node', target: 'join_node', targetHandle: 'in' },
				{ id: 'e_right', source: 'right_node', target: 'join_node', targetHandle: 'in' }
			],
			schemaPlane: {
				nodeSchemas: {},
				edgeSchemas: {
					e_left: {
						mode: 'table',
						columns: [
							{ name: 'id', type: 'number', nullable: false, properties: {} },
							{ name: 'left_only', type: 'string', nullable: true, properties: {} }
						]
					},
					e_right: {
						mode: 'table',
						columns: [
							{ name: 'id', type: 'number', nullable: false, properties: {} },
							{ name: 'right_only', type: 'string', nullable: true, properties: {} }
						]
					}
				}
			}
		};
		const manager = makeManager(state);
		const hints = manager.getConfigurationHints('join_node');
		expect(hints.suggestions?.['join.availableKeys']).toEqual(['id']);
	});

	it('derives aggregate.numericColumns from numeric table columns', () => {
		const state = {
			nodes: [{ id: 'agg_node', data: { kind: 'transform', transformKind: 'aggregate' } }],
			edges: [{ id: 'e_in', source: 'src', target: 'agg_node', targetHandle: 'in' }],
			schemaPlane: {
				nodeSchemas: {},
				edgeSchemas: {
					e_in: {
						mode: 'table',
						columns: [
							{ name: 'name', type: 'string', nullable: true, properties: {} },
							{ name: 'score', type: 'number', nullable: true, properties: {} },
							{ name: 'count', type: 'number', nullable: true, properties: {} }
						]
					}
				}
			}
		};
		const manager = makeManager(state);
		const hints = manager.getConfigurationHints('agg_node');
		expect(hints.suggestions?.['aggregate.numericColumns']).toEqual(['score', 'count']);
	});

	it('derives training input_dim and num_classes suggestions from incoming tensor properties', () => {
		const state = {
			nodes: [{ id: 'train_node', data: { kind: 'training_job' } }],
			edges: [{ id: 'e_train', source: 'up', target: 'train_node', targetHandle: 'train' }],
			schemaPlane: {
				nodeSchemas: {},
				edgeSchemas: {
					e_train: {
						mode: 'tensor',
						columns: [],
						shape: ['B', 'T', 128],
						dtype: 'float32',
						properties: { class_set: ['yes', 'no'] }
					}
				}
			}
		};
		const manager = makeManager(state);
		const hints = manager.getConfigurationHints('train_node');
		expect(hints.suggestions?.['params.architecture_config.input_dim']).toBe(128);
		expect(hints.suggestions?.['params.num_classes']).toBe(2);
		expect(hints.upstreamShape).toEqual(['B', 'T', 128]);
	});
});

