import ToolMcpEditor from './ToolMcpEditor.svelte';
import ToolHttpEditor from './ToolHttpEditor.svelte';
import ToolFunctionEditor from './ToolFunctionEditor.svelte';
import ToolPythonEditor from './ToolPythonEditor.svelte';
import ToolJsEditor from './ToolJsEditor.svelte';
import ToolShellEditor from './ToolShellEditor.svelte';
import ToolDbEditor from './ToolDbEditor.svelte';
import ToolBuiltinEditor from './ToolBuiltinEditor.svelte';

export const ToolEditorByProvider = {
	mcp: ToolMcpEditor,
	http: ToolHttpEditor,
	function: ToolFunctionEditor,
	python: ToolPythonEditor,
	js: ToolJsEditor,
	shell: ToolShellEditor,
	db: ToolDbEditor,
	builtin: ToolBuiltinEditor
} as const;
