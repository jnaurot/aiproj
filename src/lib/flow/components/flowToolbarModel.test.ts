import { describe, expect, it, vi } from 'vitest';
import {
	buildAddMenuItems,
	buildProjectMenuItems,
	buildRunSelectedMenuItems,
	dispatchAddMenuAction,
	dispatchProjectMenuAction,
	routePrimarySaveAction
} from './flowToolbarModel';

describe('flowToolbarModel', () => {
	it('builds expected Project menu items', () => {
		const items = buildProjectMenuItems();
		expect(items.map((i) => i.label)).toEqual([
			'New Graph',
			'Save Graph',
			'Save Version',
			'Save Graph As',
			'Load Graph',
			'Save as Component',
			'Import',
			'Export',
			'Delete Graph',
			'Reset'
		]);
		expect(items.find((i) => i.id === 'save_as_component')?.disabled).toBeFalsy();
		expect(items.find((i) => i.id === 'save_version')?.disabled).toBeFalsy();
		expect(items.find((i) => i.id === 'save_as_graph')?.disabled).toBeFalsy();
	});

	it('builds component-context save labels', () => {
		const items = buildProjectMenuItems('component');
		expect(items.find((i) => i.id === 'save_graph')?.label).toBe('Save Component Revision');
		expect(items.find((i) => i.id === 'save_as_component')?.label).toBe('Save as New Component');
		expect(items.find((i) => i.id === 'save_as_component')?.disabled).toBeFalsy();
		expect(items.find((i) => i.id === 'save_version')?.disabled).toBe(true);
		expect(items.find((i) => i.id === 'save_as_graph')?.disabled).toBe(true);
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
			saveGraph: vi.fn(),
			saveComponentRevision: vi.fn(),
			saveVersion: vi.fn(),
			saveGraphAs: vi.fn(),
			loadGraph: vi.fn(),
			saveAsComponent: vi.fn(),
			importGraph: vi.fn(),
			exportGraph: vi.fn(),
			deleteGraph: vi.fn(),
			reset: vi.fn()
		};
		dispatchProjectMenuAction('new_graph', 'graph', handlers);
		dispatchProjectMenuAction('save_graph', 'graph', handlers);
		dispatchProjectMenuAction('save_version', 'graph', handlers);
		dispatchProjectMenuAction('save_as_graph', 'graph', handlers);
		dispatchProjectMenuAction('load_graph', 'graph', handlers);
		dispatchProjectMenuAction('save_as_component', 'graph', handlers);
		dispatchProjectMenuAction('import_graph', 'graph', handlers);
		dispatchProjectMenuAction('export_graph', 'graph', handlers);
		dispatchProjectMenuAction('delete_graph', 'graph', handlers);
		dispatchProjectMenuAction('reset_graph', 'graph', handlers);
		expect(handlers.newGraph).toHaveBeenCalledTimes(1);
		expect(handlers.saveGraph).toHaveBeenCalledTimes(1);
		expect(handlers.saveComponentRevision).toHaveBeenCalledTimes(0);
		expect(handlers.saveVersion).toHaveBeenCalledTimes(1);
		expect(handlers.saveGraphAs).toHaveBeenCalledTimes(1);
		expect(handlers.loadGraph).toHaveBeenCalledTimes(1);
		expect(handlers.saveAsComponent).toHaveBeenCalledTimes(1);
		expect(handlers.importGraph).toHaveBeenCalledTimes(1);
		expect(handlers.exportGraph).toHaveBeenCalledTimes(1);
		expect(handlers.deleteGraph).toHaveBeenCalledTimes(1);
		expect(handlers.reset).toHaveBeenCalledTimes(1);
	});

	it('guards save actions by context at dispatch level', () => {
		const handlers = {
			newGraph: vi.fn(),
			saveGraph: vi.fn(),
			saveComponentRevision: vi.fn(),
			saveVersion: vi.fn(),
			saveGraphAs: vi.fn(),
			loadGraph: vi.fn(),
			saveAsComponent: vi.fn(),
			importGraph: vi.fn(),
			exportGraph: vi.fn(),
			deleteGraph: vi.fn(),
			reset: vi.fn()
		};
		dispatchProjectMenuAction('save_version', 'component', handlers);
		dispatchProjectMenuAction('save_as_graph', 'component', handlers);
		expect(handlers.saveVersion).toHaveBeenCalledTimes(0);
		expect(handlers.saveGraphAs).toHaveBeenCalledTimes(0);
		expect(handlers.saveAsComponent).toHaveBeenCalledTimes(0);
	});

	it('routes primary save by context', () => {
		const handlers = {
			saveGraph: vi.fn(),
			saveComponentRevision: vi.fn()
		};
		routePrimarySaveAction('graph', handlers);
		expect(handlers.saveGraph).toHaveBeenCalledTimes(1);
		expect(handlers.saveComponentRevision).toHaveBeenCalledTimes(0);
		routePrimarySaveAction('component', handlers);
		expect(handlers.saveGraph).toHaveBeenCalledTimes(1);
		expect(handlers.saveComponentRevision).toHaveBeenCalledTimes(1);
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
