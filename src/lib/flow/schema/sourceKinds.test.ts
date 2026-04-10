import { describe, expect, it } from 'vitest';
import {
	SourceNodeDataSchema,
	SourceFileParamsSchema,
	SourceObjectStoreParamsSchema,
	SourceWarehouseParamsSchema
} from '$lib/flow/schema/source';
import {
	defaultSourceMetaByKind,
	defaultSourceParamsByKind
} from '$lib/flow/schema/sourceDefaults';

describe('source kinds schema coverage', () => {
	it('parses object_store params with defaults', () => {
		const parsed = SourceObjectStoreParamsSchema.parse({
			provider: 's3',
			object_store_mode: 'mock',
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
			object_store_mode: 'mock',
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
			params: { provider: 's3', object_store_mode: 'mock', bucket: 'demo', key: 'file.txt', file_format: 'txt' }
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

	it('marks api source kind as non-memoizable by default', () => {
		expect(defaultSourceMetaByKind.api.memoizable).toBe(false);
		expect(defaultSourceMetaByKind.file.memoizable).toBeUndefined();
	});

	it('parses csv dialect and locale controls', () => {
		const parsed = SourceFileParamsSchema.parse({
			rel_path: '.',
			filename: 'data.csv',
			file_format: 'csv',
			delimiter: ';',
			has_header: true,
			quote_char: '"',
			escape_char: '\\',
			malformed_row_policy: 'warn',
			decimal_separator: ',',
			thousands_separator: '.',
			date_columns: ['created_at'],
			date_format: '%d.%m.%Y',
			json_mode: 'auto',
			json_streaming_enabled: true,
			json_stream_chunk_lines: 500,
			json_stream_max_records: 1000,
			json_flatten_strategy: 'deep',
			json_flatten_separator: '_',
			excel_import_strategy: 'stack',
			excel_sheets: ['Sheet1', 'Sheet2'],
			txt_record_mode: 'fixed_chunk',
			txt_chunk_size: 128,
			pdf_extraction_mode: 'hybrid',
			image_extract_metadata: true,
			image_svg_policy: 'sanitize',
			image_tiff_pages_mode: 'all',
			audio_extract_metadata: true,
			audio_normalize: true,
			audio_target_peak: 0.8,
			audio_transcode_format: 'wav',
			video_extract_metadata: true,
			video_frame_mode: 'interval',
			video_frame_interval_sec: 2,
			video_max_frames: 7,
			parquet_columns: ['id'],
			parquet_row_groups: [0],
			parquet_max_rows: 100
		});
		expect(parsed.quote_char).toBe('"');
		expect(parsed.escape_char).toBe('\\');
		expect(parsed.malformed_row_policy).toBe('warn');
		expect(parsed.decimal_separator).toBe(',');
		expect(parsed.date_columns).toEqual(['created_at']);
		expect(parsed.parquet_columns).toEqual(['id']);
		expect(parsed.parquet_row_groups).toEqual([0]);
		expect(parsed.parquet_max_rows).toBe(100);
		expect(parsed.json_mode).toBe('auto');
		expect(parsed.json_streaming_enabled).toBe(true);
		expect(parsed.json_stream_chunk_lines).toBe(500);
		expect(parsed.json_stream_max_records).toBe(1000);
		expect(parsed.json_flatten_strategy).toBe('deep');
		expect(parsed.json_flatten_separator).toBe('_');
		expect(parsed.excel_import_strategy).toBe('stack');
		expect(parsed.excel_sheets).toEqual(['Sheet1', 'Sheet2']);
		expect(parsed.txt_record_mode).toBe('fixed_chunk');
		expect(parsed.txt_chunk_size).toBe(128);
		expect(parsed.pdf_extraction_mode).toBe('hybrid');
		expect(parsed.image_extract_metadata).toBe(true);
		expect(parsed.image_svg_policy).toBe('sanitize');
		expect(parsed.image_tiff_pages_mode).toBe('all');
		expect(parsed.audio_extract_metadata).toBe(true);
		expect(parsed.audio_normalize).toBe(true);
		expect(parsed.audio_target_peak).toBe(0.8);
		expect(parsed.audio_transcode_format).toBe('wav');
		expect(parsed.video_extract_metadata).toBe(true);
		expect(parsed.video_frame_mode).toBe('interval');
		expect(parsed.video_frame_interval_sec).toBe(2);
		expect(parsed.video_max_frames).toBe(7);
	});
});
