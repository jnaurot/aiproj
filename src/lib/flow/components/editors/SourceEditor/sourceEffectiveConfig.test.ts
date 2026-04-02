import { describe, expect, it, test } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';

import {
	effectiveConfigForSource,
	fileAutoAdjustmentNotices
} from '$lib/flow/components/editors/SourceEditor/sourceEffectiveConfig';

describe('source effective config + auto adjustment helpers', () => {
	test('test_source_file_auto_adjustment_notice_rendered_on_format_change', () => {
		const notices = fileAutoAdjustmentNotices(
			{
				file_format: 'txt',
				output: { mode: 'text' } as any
			},
			{
				file_format: 'csv',
				delimiter: ',',
				output: { mode: 'table' } as any
			},
			'Format auto-adjustment'
		);
		expect(notices.some((notice) => notice.includes('output.mode auto-set to table'))).toBe(true);
		expect(notices.some((notice) => notice.includes('delimiter normalized'))).toBe(true);
	});

	test('test_source_editor_effective_preview_renders_all_supported_kinds', () => {
		const cases = [
			{ kind: 'file', params: { file_format: 'csv', output: { mode: 'table' } } },
			{
				kind: 'database',
				params: { connection_ref: 'conn:db', query: 'select 1', output: { mode: 'json' } }
			},
			{ kind: 'api', params: { method: 'POST', bodyMode: 'json', auth_type: 'none' } },
			{
				kind: 'object_store',
				params: { provider: 's3', object_store_mode: 'provider', connection_ref: 'conn:os' }
			},
			{
				kind: 'warehouse',
				params: { provider: 'snowflake', query: 'select 1', output: { mode: 'table' } }
			}
		] as const;
		for (const entry of cases) {
			const lines = effectiveConfigForSource(entry.kind, entry.params as Record<string, unknown>);
			expect(lines.length).toBeGreaterThan(0);
			expect(lines.every((line) => line.key.length > 0 && String(line.value).length > 0)).toBe(true);
		}
	});

	test('test_source_effective_preview_matches_saved_params_after_apply', () => {
		const lines = effectiveConfigForSource('database', {
			connection_ref: 'conn:db',
			query: 'select * from jobs',
			output: { mode: 'json' }
		});
		expect(lines).toEqual([
			{ key: 'output', value: 'json' },
			{ key: 'connection', value: 'connection_ref' },
			{ key: 'input', value: 'query' }
		]);
	});

	it('test_source_editor_recent_adjustments_log_updates_on_auto_changes', () => {
		const sourceFileEditorPath = path.resolve(
			'src/lib/flow/components/editors/SourceEditor/SourceFileEditor.svelte'
		);
		const source = fs.readFileSync(sourceFileEditorPath, 'utf-8');
		expect(source).toContain('function pushAdjustments(entries: string[]): void');
		expect(source).toContain('recentAdjustmentsByNode');
		expect(source).toContain('recentAdjustments={recentAdjustments}');
	});
});

