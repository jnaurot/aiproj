import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('NodeInspector component editor context wiring', () => {
	it('passes editingContext to ComponentEditor', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/components/NodeInspector.svelte');
		const text = readFileSync(file, 'utf8');
		expect(text.includes('editingContext={$graphStore.editingContext}')).toBe(true);
	});

	it('renders component contract editor in component-edit context using session draft params', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/components/NodeInspector.svelte');
		const text = readFileSync(file, 'utf8');
		expect(text.includes('isComponentEditContext && componentSessionContractParams && !isComponent')).toBe(true);
		expect(text.includes('params={componentSessionContractParams}')).toBe(true);
		expect(text.includes('onDraft={onComponentSessionContractDraft}')).toBe(true);
		expect(text.includes('showComponentMetaSection={false}')).toBe(true);
		expect(text.includes('Component Contract (Authoring)')).toBe(true);
	});

	it('renders ConfigurationOracle for transform inspector mode', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/components/NodeInspector.svelte');
		const text = readFileSync(file, 'utf8');
		expect(text.includes("import ConfigurationOracle from '$lib/flow/components/ui/ConfigurationOracle.svelte';")).toBe(
			true
		);
		expect(text.includes('<ConfigurationOracle')).toBe(true);
		expect(text.includes('schemaConfigurationHints')).toBe(true);
	});
});
