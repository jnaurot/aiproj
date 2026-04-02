import { describe, expect, test } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';

const SOURCE_EDITOR_DIR = path.resolve('src/lib/flow/components/editors/SourceEditor');
const THEMED_SELECT_PATH = path.resolve('src/lib/flow/components/ui/ThemedSelect.svelte');

function read(filePath: string): string {
	return fs.readFileSync(filePath, 'utf-8');
}

function sourceEditorSvelteFiles(): string[] {
	return fs
		.readdirSync(SOURCE_EDITOR_DIR)
		.filter((name) => name.endsWith('.svelte'))
		.map((name) => path.join(SOURCE_EDITOR_DIR, name));
}

describe('SourceEditor themed select compliance', () => {
	test('test_source_editors_use_themed_select_for_all_dropdown_controls', () => {
		const files = sourceEditorSvelteFiles();
		const offenders: string[] = [];
		for (const filePath of files) {
			const content = read(filePath);
			if (/<select\b/i.test(content)) offenders.push(path.basename(filePath));
		}
		expect(offenders).toEqual([]);
	});

	test('test_source_editor_dropdown_dark_theme_readability', () => {
		const content = read(THEMED_SELECT_PATH);
		expect(content).toContain('var(--color-control-option-bg)');
		expect(content).toContain('var(--color-control-option-text)');
		expect(content).toContain('var(--color-control-option-hover-bg)');
		expect(content).toContain('var(--color-control-option-selected-bg)');
	});

	test('test_source_editor_dropdown_light_theme_readability', () => {
		const content = read(THEMED_SELECT_PATH);
		expect(content).toContain('var(--color-control-bg)');
		expect(content).toContain('var(--color-control-text)');
		expect(content).toContain('var(--color-control-border)');
	});

	test('test_source_editor_dropdown_keyboard_navigation_and_aria', () => {
		const content = read(THEMED_SELECT_PATH);
		expect(content).toContain('aria-haspopup="listbox"');
		expect(content).toContain('aria-expanded={open}');
		expect(content).toContain('on:keydown={handleTriggerKeydown}');
		expect(content).toContain('on:keydown={(event) => handleOptionKeydown(idx, event)}');
	});

	test('test_source_editor_no_raw_select_elements_guardrail', () => {
		const files = sourceEditorSvelteFiles();
		const offenders: string[] = [];
		for (const filePath of files) {
			const content = read(filePath);
			if (/<select\b/i.test(content)) offenders.push(filePath);
		}
		if (offenders.length > 0) {
			throw new Error(`Raw <select> controls found in SourceEditor files: ${offenders.join(', ')}`);
		}
	});
});
