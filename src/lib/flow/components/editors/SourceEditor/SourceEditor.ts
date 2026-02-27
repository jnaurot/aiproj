//lib/flow/components/editors/SourceEditor/SourceEditor.ts
import SourceFileEditor from './SourceFileEditor.svelte';
import SourceDatabaseEditor from './SourceDatabaseEditor.svelte';
import SourceApiEditor from './SourceAPIEditor.svelte';

export const SourceEditorByKind = {
    file: SourceFileEditor,
    database: SourceDatabaseEditor,
    api: SourceApiEditor
} as const;
