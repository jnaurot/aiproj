import type { ToolbarMenuItem } from './toolbarMenu';

export function buildProjectMenuItems(): ToolbarMenuItem[] {
	return [
		{ id: 'new_graph', label: 'New Graph' },
		{ id: 'save_graph', label: 'Save' },
		{ id: 'save_as_graph', label: 'Save As' },
		{ id: 'save_as_component', label: 'Save as Component' },
		{ id: 'import_graph', label: 'Import' },
		{ id: 'export_graph', label: 'Export' },
		{ id: 'history_graph', label: 'History' },
		{ id: 'reset_graph', label: 'Reset', danger: true }
	];
}

export function buildAddMenuItems(hasPresets: boolean): ToolbarMenuItem[] {
	return [
		{ id: 'add_source', label: 'Source' },
		{ id: 'add_transform', label: 'Transform' },
		{ id: 'add_llm', label: 'LLM' },
		{ id: 'add_tool', label: 'Tool' },
		{ id: 'add_from_preset', label: 'Preset', disabled: !hasPresets },
		{ id: 'add_component', label: 'Component' },
	];
}

export function buildRunSelectedMenuItems(hasSelectedNode: boolean): ToolbarMenuItem[] {
	return [
		{
			id: 'run_from_selected',
			label: 'Run from selected',
			disabled: !hasSelectedNode
		}
	];
}

export type ProjectToolbarHandlers = {
	newGraph: () => void;
	save: () => void;
	saveAs: () => void;
	saveAsComponent: () => void;
	importGraph: () => void;
	exportGraph: () => void;
	history: () => void;
	reset: () => void;
};

export function dispatchProjectMenuAction(actionId: string, handlers: ProjectToolbarHandlers): void {
	if (actionId === 'new_graph') handlers.newGraph();
	if (actionId === 'save_graph') handlers.save();
	if (actionId === 'save_as_graph') handlers.saveAs();
	if (actionId === 'save_as_component') handlers.saveAsComponent();
	if (actionId === 'import_graph') handlers.importGraph();
	if (actionId === 'export_graph') handlers.exportGraph();
	if (actionId === 'history_graph') handlers.history();
	if (actionId === 'reset_graph') handlers.reset();
}

export type AddToolbarHandlers = {
	addSource: () => void;
	addTransform: () => void;
	addLlm: () => void;
	addTool: () => void;
	addComponent: () => void;
	addFromPreset: () => void;
};

export function dispatchAddMenuAction(actionId: string, handlers: AddToolbarHandlers): void {
	if (actionId === 'add_source') handlers.addSource();
	if (actionId === 'add_transform') handlers.addTransform();
	if (actionId === 'add_llm') handlers.addLlm();
	if (actionId === 'add_tool') handlers.addTool();
	if (actionId === 'add_component') handlers.addComponent();
	if (actionId === 'add_from_preset') handlers.addFromPreset();
}
