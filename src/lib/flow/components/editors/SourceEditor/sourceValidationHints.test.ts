import { describe, expect, test } from 'vitest';

import {
	sourceApiValidationHints,
	sourceControlFromParamPath,
	sourceDatabaseValidationHints
} from '$lib/flow/components/editors/SourceEditor/sourceValidationHints';

describe('source editor validation hints', () => {
	test('test_source_database_inline_validation_for_connection_and_query_requirements', () => {
		const hints = sourceDatabaseValidationHints({
			connection_string: '',
			connection_ref: '',
			query: '',
			table_name: ''
		});
		expect(hints.some((hint) => hint.controlId === 'connection' && hint.level === 'error')).toBe(true);
		expect(hints.some((hint) => hint.controlId === 'input' && hint.level === 'error')).toBe(true);
	});

	test('test_source_api_inline_validation_for_body_mode_and_content_type', () => {
		const hints = sourceApiValidationHints({
			bodyMode: 'json',
			contentType: 'text/plain'
		});
		expect(hints.some((hint) => hint.controlId === 'content_type')).toBe(true);
	});

	test('test_source_editor_param_path_error_maps_to_control_highlight', () => {
		expect(sourceControlFromParamPath('database', 'connection_ref')).toBe('connection');
		expect(sourceControlFromParamPath('database', 'query')).toBe('input');
		expect(sourceControlFromParamPath('api', 'contentType')).toBe('content_type');
		expect(sourceControlFromParamPath('api', 'auth_token_ref')).toBe('auth');
	});

	test('test_source_editor_validation_hints_render_pre_run', () => {
		const dbHints = sourceDatabaseValidationHints({
			connection_ref: 'conn:db',
			query: ''
		});
		expect(dbHints.some((hint) => hint.message.includes('query or table_name'))).toBe(true);
	});

	test('test_source_backend_validation_error_surfaces_at_correct_frontend_control', () => {
		const mapped = sourceControlFromParamPath('database', 'table_name');
		expect(mapped).toBe('input');
	});
});

