import { describe, expect, test } from 'vitest';

import {
	applyGuidedPatchPreservingAdvanced,
	guidedToFullRoundtrip,
	guidedWhitelistForSource
} from '$lib/flow/components/editors/SourceEditor/sourceGuidedParity';

describe('guided source editor parity', () => {
	test('test_source_guided_field_whitelist_per_kind', () => {
		expect(guidedWhitelistForSource('file')).toContain('file_format');
		expect(guidedWhitelistForSource('database')).toContain('query');
		expect(guidedWhitelistForSource('api')).toContain('url');
	});

	test('test_source_guided_to_full_editor_roundtrip_no_param_loss', () => {
		const current = {
			method: 'GET',
			url: 'https://example.com',
			retry: { max_attempts: 3 },
			cache_policy: { mode: 'ttl', ttl_seconds: 60 }
		};
		const next = guidedToFullRoundtrip(current, { method: 'POST' });
		expect(next.retry).toEqual({ max_attempts: 3 });
		expect(next.cache_policy).toEqual({ mode: 'ttl', ttl_seconds: 60 });
		expect(next.method).toBe('POST');
	});

	test('test_source_full_to_guided_preserves_advanced_params', () => {
		const next = applyGuidedPatchPreservingAdvanced(
			{
				connection_ref: 'conn:db',
				incremental: { enabled: true, cursor_column: 'updated_at' }
			},
			{ connection_ref: 'conn:new-db' }
		);
		expect((next.incremental as any).cursor_column).toBe('updated_at');
	});

	test('test_source_guided_open_full_editor_preserves_draft_state', () => {
		const draft = guidedToFullRoundtrip(
			{ query: 'select 1', limit: 100, output: { mode: 'table' } },
			{ query: 'select 2' }
		);
		expect(draft.query).toBe('select 2');
		expect((draft.output as any).mode).toBe('table');
	});
});

