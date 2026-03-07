import { describe, expect, it, vi } from 'vitest';
import {
	buildAddMenuItems,
	buildProjectMenuItems,
	buildRunSelectedMenuItems,
	dispatchAddMenuAction,
	dispatchProjectMenuAction
} from './flowToolbarModel';

describe('flowToolbarModel', () => {
	it('builds expected Project menu items', () => {
		const items = buildProjectMenuItems();
		expect(items.map((i) => i.label)).toEqual([
			'New Graph',
			'Save',
			'Save As',
			'Import',
			'Export',
			'History',
			'Reset'
		]);
	});

	it('builds expected Add menu items with Preset before Component', () => {
		const items = buildAddMenuItems(true);
		expect(items.map((i) => i.label)).toEqual([
			'Source',
			'Transform',
			'LLM',
			'Tool',
			'Preset',
			'Component',
		]);
		expect(buildAddMenuItems(false).find((i) => i.id === 'add_from_preset')?.disabled).toBe(true);
	});

	it('respects selected-node disabled state for Run from selected', () => {
		expect(buildRunSelectedMenuItems(false)[0]?.disabled).toBe(true);
		expect(buildRunSelectedMenuItems(true)[0]?.disabled).toBe(false);
	});

	it('dispatches project actions to handlers', () => {
		const handlers = {
			newGraph: vi.fn(),
			save: vi.fn(),
			saveAs: vi.fn(),
			importGraph: vi.fn(),
			exportGraph: vi.fn(),
			history: vi.fn(),
			reset: vi.fn()
		};
		dispatchProjectMenuAction('new_graph', handlers);
		dispatchProjectMenuAction('save_graph', handlers);
		dispatchProjectMenuAction('save_as_graph', handlers);
		dispatchProjectMenuAction('import_graph', handlers);
		dispatchProjectMenuAction('export_graph', handlers);
		dispatchProjectMenuAction('history_graph', handlers);
		dispatchProjectMenuAction('reset_graph', handlers);
		expect(handlers.newGraph).toHaveBeenCalledTimes(1);
		expect(handlers.save).toHaveBeenCalledTimes(1);
		expect(handlers.saveAs).toHaveBeenCalledTimes(1);
		expect(handlers.importGraph).toHaveBeenCalledTimes(1);
		expect(handlers.exportGraph).toHaveBeenCalledTimes(1);
		expect(handlers.history).toHaveBeenCalledTimes(1);
		expect(handlers.reset).toHaveBeenCalledTimes(1);
	});

	it('dispatches add actions including component', () => {
		const handlers = {
			addSource: vi.fn(),
			addTransform: vi.fn(),
			addLlm: vi.fn(),
			addTool: vi.fn(),
			addComponent: vi.fn(),
			addFromPreset: vi.fn()
		};
		dispatchAddMenuAction('add_source', handlers);
		dispatchAddMenuAction('add_transform', handlers);
		dispatchAddMenuAction('add_llm', handlers);
		dispatchAddMenuAction('add_tool', handlers);
		dispatchAddMenuAction('add_component', handlers);
		dispatchAddMenuAction('add_from_preset', handlers);
		expect(handlers.addSource).toHaveBeenCalledTimes(1);
		expect(handlers.addTransform).toHaveBeenCalledTimes(1);
		expect(handlers.addLlm).toHaveBeenCalledTimes(1);
		expect(handlers.addTool).toHaveBeenCalledTimes(1);
		expect(handlers.addComponent).toHaveBeenCalledTimes(1);
		expect(handlers.addFromPreset).toHaveBeenCalledTimes(1);
	});
});
