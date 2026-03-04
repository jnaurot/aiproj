//lib/flow/components/editors/TransformEditor/TransformEditor.ts
import TransformFilterEditor from './TransformFilterEditor.svelte';
import TransformSelectEditor from './TransformSelectEditor.svelte';
import TransformRenameEditor from './TransformRenameEditor.svelte';
import TransformDeriveEditor from './TransformDeriveEditor.svelte';
import TransformAggregateEditor from './TransformAggregateEditor.svelte';
import TransformJoinEditor from './TransformJoinEditor.svelte';
import TransformSortEditor from './TransformSortEditor.svelte';
import TransformLimitEditor from './TransformLimitEditor.svelte';
import TransformDedupeEditor from './TransformDedupeEditor.svelte';
import TransformSplitEditor from './TransformSplitEditor.svelte';
import TransformSqlEditor from './TransformSqlEditor.svelte';

// filter, select, rename, derive, aggregate, join, sort, limit, dedupe, sql

export type EditorCommitMode = 'draft' | 'immediate';

export const TransformEditorCommitModeByKind = {
	filter: 'draft',
	select: 'draft',
	rename: 'draft',
	derive: 'draft',
	aggregate: 'draft',
	join: 'draft',
	sort: 'draft',
	limit: 'immediate',
	dedupe: 'immediate',
	split: 'draft',
	sql: 'draft'
} as const satisfies Record<string, EditorCommitMode>;

export const TransformEditorByKind = {
    filter: TransformFilterEditor,
    select: TransformSelectEditor,
    rename: TransformRenameEditor,
    derive: TransformDeriveEditor,
    aggregate: TransformAggregateEditor,
    join: TransformJoinEditor,
    sort: TransformSortEditor,
    limit: TransformLimitEditor,
    dedupe: TransformDedupeEditor,
    split: TransformSplitEditor,
    sql: TransformSqlEditor
} as const;
