//lib/flow/components/editors/SourceEditor/SourceEditor.ts
import SourceFileEditor from './SourceFileEditor.svelte';
import SourceDatabaseEditor from './SourceDatabaseEditor.svelte';
import SourcApieEditor from './SourceAPIEditor.svelte';

export const SourceEditorByKind = {
    file: SourceFileEditor,
    database: SourceDatabaseEditor,
    api: SourcApieEditor
} as const;