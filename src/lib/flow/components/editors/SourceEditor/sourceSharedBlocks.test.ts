import { describe, expect, test } from 'vitest';

import {
	deepMergeWithoutParamLoss,
	withApiCacheMode,
	withDatabaseConnection,
	withIncrementalEnabled,
	withOutputMode,
	withPartitionEnabled,
	withWarehouseConnection
} from '$lib/flow/components/editors/SourceEditor/sourceSharedBlocks';

describe('source shared block patch helpers', () => {
	test('test_source_output_mode_block_roundtrip_all_source_kinds', () => {
		const file = withOutputMode({ output: { mode: 'text' } }, 'json');
		const db = withOutputMode({ output: { mode: 'table' } }, 'binary');
		const api = withOutputMode({ output: { mode: 'json' } }, 'text');
		const store = withOutputMode({ output: { mode: 'text' } }, 'table');
		const warehouse = withOutputMode({ output: { mode: 'table' } }, 'json');
		expect((file.output as any).mode).toBe('json');
		expect((db.output as any).mode).toBe('binary');
		expect((api.output as any).mode).toBe('text');
		expect((store.output as any).mode).toBe('table');
		expect((warehouse.output as any).mode).toBe('json');
	});

	test('test_source_connection_block_roundtrip_database_and_warehouse', () => {
		const db = withDatabaseConnection({}, { connection_ref: 'conn:db' });
		const warehouse = withWarehouseConnection({}, { connection_string: 'warehouse://test' });
		expect(db.connection_ref).toBe('conn:db');
		expect(warehouse.connection_string).toBe('warehouse://test');
	});

	test('test_source_incremental_block_roundtrip_database_and_api', () => {
		const db = withIncrementalEnabled({ incremental: { cursor_column: 'id' } }, true);
		const api = withIncrementalEnabled({ incremental: { state_key: 'api-state' } }, false);
		expect((db.incremental as any).enabled).toBe(true);
		expect((api.incremental as any).enabled).toBe(false);
	});

	test('test_source_partition_block_roundtrip_database_and_api', () => {
		const db = withPartitionEnabled({ partition: { kind: 'static_list' } }, true);
		const api = withPartitionEnabled({ partition: { bind_key: 'bucket' } }, false);
		expect((db.partition as any).enabled).toBe(true);
		expect((api.partition as any).enabled).toBe(false);
	});

	test('test_source_cache_retry_block_roundtrip_api', () => {
		const patch = withApiCacheMode({ cache_policy: { mode: 'default' } }, 'ttl', 120);
		expect((patch.cache_policy as any).mode).toBe('ttl');
		expect((patch.cache_policy as any).ttl_seconds).toBe(120);
	});

	test('test_source_shared_blocks_emit_identical_patch_shapes_as_legacy', () => {
		const outputPatch = withOutputMode({ output: { mode: 'text', strict: true } }, 'json') as any;
		expect(outputPatch).toEqual({
			output: { mode: 'json', strict: true }
		});
	});

	test('test_source_shared_blocks_no_param_loss_on_save_restore', () => {
		const merged = deepMergeWithoutParamLoss(
			{
				output: { mode: 'text', strict: true },
				retry: { max_attempts: 2, jitter_seconds: 0.1 },
				connection_ref: 'conn:demo'
			},
			{
				output: { mode: 'json' },
				retry: { max_attempts: 4 }
			}
		);
		expect(merged).toEqual({
			output: { mode: 'json', strict: true },
			retry: { max_attempts: 4, jitter_seconds: 0.1 },
			connection_ref: 'conn:demo'
		});
	});
});

