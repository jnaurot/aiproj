import { describe, expect, test } from 'vitest';

import {
	defaultSourceSectionOpenState,
	sourceFileFormatCollapsedGroups,
	sourceSectionLayoutForKind
} from '$lib/flow/components/editors/SourceEditor/sourceEditorLayout';
import {
	ensureSourceDisclosureState,
	patchSourceDisclosureState
} from '$lib/flow/components/editors/SourceEditor/sourceDisclosureState';

describe('source editor canonical section layout', () => {
	test('test_source_editor_file_uses_canonical_section_order', () => {
		const sections = sourceSectionLayoutForKind('file').map((section) => section.title);
		expect(sections).toEqual(['Connection', 'Input', 'Parsing', 'Execution', 'Advanced', 'Debug']);
	});

	test('test_source_editor_database_uses_canonical_section_order', () => {
		const sections = sourceSectionLayoutForKind('database').map((section) => section.title);
		expect(sections).toEqual(['Connection', 'Input', 'Execution', 'Advanced', 'Debug']);
	});

	test('test_source_editor_object_store_uses_canonical_section_order', () => {
		const sections = sourceSectionLayoutForKind('object_store').map((section) => section.title);
		expect(sections).toEqual(['Connection', 'Input', 'Parsing', 'Execution', 'Advanced', 'Debug']);
	});

	test('test_source_editor_warehouse_uses_canonical_section_order', () => {
		const sections = sourceSectionLayoutForKind('warehouse').map((section) => section.title);
		expect(sections).toEqual(['Connection', 'Input', 'Execution', 'Advanced', 'Debug']);
	});

	test('test_source_editor_api_maps_to_canonical_section_order', () => {
		const sections = sourceSectionLayoutForKind('api').map((section) => section.title);
		expect(sections).toEqual(['Connection', 'Input', 'Parsing', 'Execution', 'Advanced', 'Debug']);
	});

	test('test_source_editor_default_open_sections_are_connection_input_execution', () => {
		const defaults = defaultSourceSectionOpenState('api');
		expect(defaults.connection).toBe(true);
		expect(defaults.input).toBe(true);
		expect(defaults.execution).toBe(true);
	});

	test('test_source_editor_default_collapsed_sections_are_advanced_debug', () => {
		const defaults = defaultSourceSectionOpenState('file');
		expect(defaults.advanced).toBe(false);
		expect(defaults.debug).toBe(false);
	});

	test('test_source_file_editor_format_specific_controls_collapsed_by_default', () => {
		expect(sourceFileFormatCollapsedGroups('csv')).toContain('csv_group');
		expect(sourceFileFormatCollapsedGroups('json')).toContain('json_group');
		expect(sourceFileFormatCollapsedGroups('mp3')).toContain('audio_group');
	});

	test('test_source_editor_disclosure_state_persists_per_node', () => {
		const store = {};
		const initial = ensureSourceDisclosureState(store, 'nodeA', 'api');
		expect(initial.connection).toBe(true);
		patchSourceDisclosureState(store, 'nodeA', 'api', { advanced: true });
		const persisted = ensureSourceDisclosureState(store, 'nodeA', 'api');
		expect(persisted.advanced).toBe(true);
	});

	test('test_source_editor_kind_switch_preserves_section_order_and_visibility', () => {
		const apiSections = sourceSectionLayoutForKind('api').map((section) => section.id);
		const dbSections = sourceSectionLayoutForKind('database').map((section) => section.id);
		expect(apiSections).toContain('parsing');
		expect(dbSections).not.toContain('parsing');
		expect(apiSections.slice(0, 3)).toEqual(['connection', 'input', 'parsing']);
		expect(dbSections.slice(0, 3)).toEqual(['connection', 'input', 'execution']);
	});
});

