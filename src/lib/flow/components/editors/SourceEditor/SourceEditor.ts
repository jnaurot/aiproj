//lib/flow/components/editors/SourceEditor/SourceEditor.ts
import SourceFileEditor from './SourceFileEditor.svelte';
import SourceDatabaseEditor from './SourceDatabaseEditor.svelte';
import SourceApiEditor from './SourceAPIEditor.svelte';
import SourceObjectStoreEditor from './SourceObjectStoreEditor.svelte';
import SourceWarehouseEditor from './SourceWarehouseEditor.svelte';

export const SourceEditorByKind = {
	file: SourceFileEditor,
	database: SourceDatabaseEditor,
	api: SourceApiEditor,
	object_store: SourceObjectStoreEditor,
	warehouse: SourceWarehouseEditor
} as const;
