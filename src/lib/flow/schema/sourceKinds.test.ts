import { describe, expect, it } from 'vitest';
import {
	SourceNodeDataSchema,
	SourceObjectStoreParamsSchema,
	SourceWarehouseParamsSchema
} from '$lib/flow/schema/source';
import { defaultSourceParamsByKind } from '$lib/flow/schema/sourceDefaults';

describe('source kinds schema coverage', () => {
	it('parses object_store params with defaults', () => {
		const parsed = SourceObjectStoreParamsSchema.parse({
			provider: 's3',
			bucket: 'demo',
			key: 'file.txt',
			file_format: 'txt'
		});
		expect(parsed.output.mode).toBe('text');
		expect(parsed.priming.enabled).toBe(false);
	});

	it('parses warehouse params with defaults', () => {
		const parsed = SourceWarehouseParamsSchema.parse({
			provider: 'snowflake',
			connection_ref: 'conn:warehouse_default',
			query: 'select 1'
		});
		expect(parsed.output.mode).toBe('table');
		expect(parsed.priming.mode).toBe('advisory');
	});

	it('accepts explicit priming config', () => {
		const parsed = SourceObjectStoreParamsSchema.parse({
			provider: 's3',
			bucket: 'demo',
			key: 'file.txt',
			file_format: 'txt',
			priming: {
				enabled: true,
				mode: 'priming_only',
				sample_rows: 12,
				sample_bytes: 2048,
				timeout_ms: 300
			}
		});
		expect(parsed.priming.enabled).toBe(true);
		expect(parsed.priming.mode).toBe('priming_only');
	});

	it('accepts source node data for new source kinds', () => {
		const objectNode = SourceNodeDataSchema.parse({
			kind: 'source',
			label: 'Source',
			status: 'idle',
			sourceKind: 'object_store',
			params: { provider: 's3', bucket: 'demo', key: 'file.txt', file_format: 'txt' }
		});
		const warehouseNode = SourceNodeDataSchema.parse({
			kind: 'source',
			label: 'Source',
			status: 'idle',
			sourceKind: 'warehouse',
			params: { provider: 'snowflake', connection_ref: 'conn:warehouse_default', query: 'select 1' }
		});
		expect(objectNode.sourceKind).toBe('object_store');
		expect(warehouseNode.sourceKind).toBe('warehouse');
	});

	it('exposes defaults for new source kinds', () => {
		expect(defaultSourceParamsByKind.object_store.provider).toBeDefined();
		expect(defaultSourceParamsByKind.warehouse.provider).toBeDefined();
	});
});
