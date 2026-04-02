import { describe, expect, test } from 'vitest';

import {
	makeAutoAdjustmentEvent,
	makeSectionToggleEvent,
	makeValidationEvent
} from '$lib/flow/components/editors/SourceEditor/sourceEditorTelemetry';

describe('source editor telemetry helpers', () => {
	test('test_source_editor_telemetry_emits_section_toggle_events', () => {
		const event = makeSectionToggleEvent('api', 'node-1', 'advanced', true);
		expect(event).toEqual({
			type: 'section_toggle',
			sourceKind: 'api',
			nodeId: 'node-1',
			sectionId: 'advanced',
			open: true
		});
	});

	test('test_source_editor_telemetry_emits_validation_events', () => {
		const event = makeValidationEvent('database', 'node-2', 'connection', 'error', 'shown');
		expect(event.type).toBe('validation');
		expect(event.severity).toBe('error');
	});

	test('test_source_editor_telemetry_auto_adjustment_event_redacts_sensitive_values', () => {
		const event = makeAutoAdjustmentEvent('file', 'node-3', 'format->csv', {
			connection_string: 'postgres://secret',
			auth_token_ref: 'TOKEN_VALUE',
			file_format: 'csv'
		});
		expect((event as any).redactedContext.connection_string).toBe('[REDACTED]');
		expect((event as any).redactedContext.auth_token_ref).toBe('[REDACTED]');
		expect((event as any).redactedContext.file_format).toBe('csv');
	});

	test('test_source_editor_telemetry_end_to_end_event_shape_stability', () => {
		const events = [
			makeSectionToggleEvent('api', 'node-1', 'connection', true),
			makeValidationEvent('api', 'node-1', 'content_type', 'warning', 'shown'),
			makeAutoAdjustmentEvent('api', 'node-1', 'content-type-implied-body-mode', {
				contentType: 'application/json'
			})
		];
		expect(events[0]).toHaveProperty('type');
		expect(events[1]).toHaveProperty('controlId');
		expect(events[2]).toHaveProperty('redactedContext');
	});
});

