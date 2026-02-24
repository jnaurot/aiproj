<script lang="ts">
	import { onMount } from 'svelte';
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData, TransformParams } from '$lib/flow/types/transform';
	import { TransformEditorByKind } from './TransformEditor';

	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformParams;
	export let onDraft: (patch: Record<string, any>) => void;
	export let onCommit: (patch: Record<string, any>) => void;

	let currentOp: TransformParams['op'] = 'filter';
	let EditorComponent: any = null;

	function makeDefaultForOp(op: TransformParams['op']): TransformParams {
		// IMPORTANT: return *exactly* the union shape for the op.
		const common = {
			enabled: params?.enabled ?? true,
			notes: params?.notes ?? '',
			cache: params?.cache ?? { enabled: false }
		};

		switch (op) {
			case 'filter':
				return { ...common, op, filter: { expr: 'length(text) > 10' } };
			case 'select':
				return { ...common, op, select: { columns: ['text'] } };
			case 'rename':
				return { ...common, op, rename: { map: { colA: 'colA_new' } } };
			case 'derive':
				return { ...common, op, derive: { columns: [{ name: 'colX', expr: 'colA * 2' }] } };
			case 'aggregate':
				return {
					...common,
					op,
					aggregate: { groupBy: ['group_col'], metrics: [{ as: 'cnt', expr: 'count(*)' }] }
				};
			case 'join':
				return {
					...common,
					op,
					join: { withNodeId: '', how: 'inner', on: [{ left: 'id', right: 'id' }] }
				};
			case 'sort':
				return { ...common, op, sort: { by: [{ col: 'colA', dir: 'asc' }] } };
			case 'limit':
				return { ...common, op, limit: { n: 100 } };
			case 'dedupe':
				return { ...common, op, dedupe: { by: ['id'] } };
			case 'sql':
				return { ...common, op, sql: { dialect: 'duckdb', query: 'select * from input' } };
			case 'python':
				return {
					...common,
					op,
					code: { language: 'python', source: '# disabled by default in backend\n' }
				};
		}
	}

	function getAvailableOps() {
		return [
			{ value: 'filter', label: 'Filter' },
			{ value: 'select', label: 'Select Columns' },
			{ value: 'rename', label: 'Rename Columns' },
			{ value: 'derive', label: 'Derive Columns' },
			{ value: 'aggregate', label: 'Aggregate' },
			{ value: 'join', label: 'Join' },
			{ value: 'sort', label: 'Sort' },
			{ value: 'limit', label: 'Limit' },
			{ value: 'dedupe', label: 'Deduplicate' },
			{ value: 'sql', label: 'SQL Query' }
		];
	}

	function refreshEditor(op: TransformParams['op']) {
		EditorComponent = TransformEditorByKind[op] ?? null;
	}

	onMount(() => {
		currentOp = (params?.op ?? 'filter') as any;
		refreshEditor(currentOp);

		// verify shape exists; if not, snap to a valid default
		if (!params || !params.op) {
			const next = makeDefaultForOp('filter');
			onDraft(next as any);
			onCommit(next as any);
		}
	});

	function handleOpChange(e: Event) {
		const op = (e.target as HTMLSelectElement).value as TransformParams['op'];
		currentOp = op;

		const next = makeDefaultForOp(op);
		refreshEditor(op);

		// replace whole params object to avoid invalid “extra blocks”
		onDraft(next as any);
		onCommit(next as any);
	}
</script>

<div class="transform-editor">
	<div class="editor-section">
		<h3>Transform Operation</h3>
		<div class="form-group">
			<label for="operation">Operation Type</label>
			<select id="operation" class="form-control" on:change={handleOpChange} bind:value={currentOp}>
				{#each getAvailableOps() as op}
					<option value={op.value}>{op.label}</option>
				{/each}
			</select>
		</div>
	</div>

	<div class="editor-section">
		<h3>Common Parameters</h3>
		<div class="form-group">
			<label>Enabled</label>
			<input
				type="checkbox"
				checked={params?.enabled ?? true}
				on:change={(e) => onDraft({ enabled: (e.target as HTMLInputElement).checked })}
			/>
		</div>

		<div class="form-group">
			<label>Notes</label>
			<textarea
				class="form-control"
				value={params?.notes ?? ''}
				on:input={(e) => onDraft({ notes: (e.target as HTMLTextAreaElement).value })}
			/>
		</div>
	</div>

	<div class="editor-section">
		<h3>Operation Parameters</h3>

		{#if EditorComponent}
			<svelte:component this={EditorComponent} {selectedNode} {params} {onDraft} {onCommit} />
		{:else}
			<div class="muted">No editor for op: {currentOp}</div>
		{/if}
	</div>
</div>

<style>
	.transform-editor {
		padding: 10px;
		font-family: Arial, sans-serif;
	}
	.editor-section {
		margin-bottom: 15px;
		padding: 10px;
		border: 1px solid #ddd;
		border-radius: 4px;
	}
	.form-group {
		margin-bottom: 10px;
	}
	.form-group label {
		display: block;
		margin-bottom: 5px;
		font-weight: bold;
	}
	.form-control {
		width: 100%;
		padding: 5px;
		border: 1px solid #ccc;
		border-radius: 3px;
		box-sizing: border-box;
	}
	.muted {
		opacity: 0.7;
		font-size: 0.9em;
	}
</style>
