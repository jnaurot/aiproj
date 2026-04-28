<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformSqlParams } from '$lib/flow/schema/transform';
	import type { InputSchemaView } from './inputSchema';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ThemedSelect, { type ThemedSelectOption } from '$lib/flow/components/ui/ThemedSelect.svelte';
	import {
		insertQuotedColumnReference,
		sqlAvailableColumns,
		summarizeSchemaAssist,
		unknownSqlReferences
	} from './sqlAssistModel';

	type Dialect = NonNullable<TransformSqlParams['dialect']>;

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformSqlParams>;
	export let onDraft: (patch: Partial<TransformSqlParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemas: InputSchemaView[] = [];

	const defaults: TransformSqlParams = {
		dialect: 'duckdb',
		query: 'SELECT * FROM input LIMIT 10',
		max_runtime_ms: 0,
		max_output_rows: 0,
		safe_mode: true
	};
	const dialectOptions: ThemedSelectOption[] = [
		{ value: 'duckdb', label: 'duckdb' },
		{ value: 'postgres', label: 'postgres' },
		{ value: 'sqlite', label: 'sqlite' }
	];

	$: void selectedNode?.id;
	$: dialect = params?.dialect ?? defaults.dialect;
	$: query = typeof params?.query === 'string' ? params.query : defaults.query;
	$: maxRuntimeMs = Number.isFinite(Number(params?.max_runtime_ms)) ? Number(params?.max_runtime_ms) : defaults.max_runtime_ms;
	$: maxOutputRows = Number.isFinite(Number(params?.max_output_rows)) ? Number(params?.max_output_rows) : defaults.max_output_rows;
	$: safeMode = typeof params?.safe_mode === 'boolean' ? params.safe_mode : defaults.safe_mode;
	$: availableColumns = sqlAvailableColumns(inputColumns, inputSchemas);
	$: unknownRefs = unknownSqlReferences(query, availableColumns);
	$: schemaAssist = summarizeSchemaAssist(inputSchemas);

	function insertSnippet(snippet: string): void {
		const merged = query.trimEnd().length > 0 ? `${query.trimEnd()}\n\n${snippet}` : snippet;
		onDraft({ query: merged });
	}

	function insertColumnName(columnName: string): void {
		onDraft({ query: insertQuotedColumnReference(query, columnName) });
	}
</script>

<Section title="SQL Query">
	<div class="hint">Write SQL against <code>input</code>.</div>
	<div class="schemaAssist">
		<div class="schemaAssistHead">
			<span>Schema Assist</span>
			<span class="schemaAssistBadge">{schemaAssist.source}/{schemaAssist.state}</span>
		</div>
		{#if availableColumns.length > 0}
			<div class="schemaCols">
				{#each availableColumns as col}
					<button class="chipBtn" type="button" on:click={() => insertColumnName(col)}>{col}</button>
				{/each}
			</div>
		{:else}
			<div class="hint">Schema unavailable (run upstream) to populate column names.</div>
		{/if}
	</div>

	<Field label="dialect">
		<ThemedSelect
			value={dialect}
			options={dialectOptions}
			ariaLabel="SQL dialect"
			onValueChange={(next) => onDraft({ dialect: next as Dialect })}
		/>
	</Field>

	<Field label="query">
		<div class="stack">
			<Input
				multiline={true}
				rows={10}
				value={query}
				placeholder={defaults.query}
				onInput={(event) => onDraft({ query: (event.currentTarget as HTMLTextAreaElement).value })}
			/>
			<div class="actions">
				<button
					class="small ghost"
					type="button"
					on:click={() => {
						onDraft(defaults);
					}}
				>
					Reset
				</button>
			</div>
		</div>
	</Field>

	<Field label="max runtime (ms)">
		<Input
			type="number"
			min={0}
			step={100}
			value={String(maxRuntimeMs)}
			onInput={(event) => {
				const raw = Number((event.currentTarget as HTMLInputElement).value);
				const next = Number.isFinite(raw) && raw >= 0 ? Math.floor(raw) : 0;
				onDraft({ max_runtime_ms: next });
			}}
		/>
	</Field>

	<Field label="max output rows">
		<Input
			type="number"
			min={0}
			step={100}
			value={String(maxOutputRows)}
			onInput={(event) => {
				const raw = Number((event.currentTarget as HTMLInputElement).value);
				const next = Number.isFinite(raw) && raw >= 0 ? Math.floor(raw) : 0;
				onDraft({ max_output_rows: next });
			}}
		/>
	</Field>

	<label class="safeModeToggle">
		<input
			type="checkbox"
			checked={safeMode}
			on:change={(event) => onDraft({ safe_mode: (event.currentTarget as HTMLInputElement).checked })}
		/>
		<span>safe mode (read-only SQL only)</span>
	</label>

	<div class="actions">
		<button class="small" type="button" on:click={() => insertSnippet('SELECT *\nFROM input\nWHERE <condition>;')}>WHERE condition</button>
		<button class="small" type="button" on:click={() => insertSnippet('SELECT CASE WHEN <condition> THEN <value_a> ELSE <value_b> END AS derived\nFROM input;')}>SELECT CASE WHEN</button>
		<button class="small" type="button" on:click={() => insertSnippet('SELECT <column>, COUNT(*) AS cnt\nFROM input\nGROUP BY <column>;')}>GROUP BY</button>
		<button
			class="small"
			type="button"
			on:click={() => insertSnippet('SELECT *\nFROM input\nWHERE length(text) > 10\nORDER BY text ASC\nLIMIT 50;')}
		>
			Filter + sort
		</button>
	</div>

	<div class="preview">
		<div class="subTitle">Preview</div>
		<pre>{query}</pre>
	</div>
	{#if unknownRefs.length > 0}
		<div class="warn">Unknown referenced columns: {unknownRefs.join(', ')}</div>
	{/if}
</Section>

<style>
	.schemaAssist {
		margin-top: 6px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		border-radius: 10px;
		padding: 8px;
		display: grid;
		gap: 6px;
	}

	.schemaAssistHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		font-size: 12px;
		font-weight: 600;
	}

	.schemaAssistBadge {
		font-size: 11px;
		padding: 2px 7px;
		border: 1px solid rgba(255, 255, 255, 0.22);
		border-radius: 999px;
		opacity: 0.9;
	}

	.schemaCols {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
	}

	.chipBtn {
		padding: 4px 8px;
		font-size: 11px;
		border-radius: 8px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		cursor: pointer;
	}

	.stack {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.subTitle {
		margin-top: 10px;
		font-size: 13px;
		font-weight: 600;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	.warn {
		font-size: 12px;
		margin-top: 8px;
		color: #fca5a5;
	}

	.actions {
		display: flex;
		gap: 8px;
		justify-content: flex-end;
		margin-top: 8px;
		flex-wrap: wrap;
	}

	.preview {
		margin-top: 12px;
	}

	pre {
		white-space: pre-wrap;
		word-break: break-word;
		padding: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		border-radius: 10px;
		font-size: 12px;
	}

	button.small {
		padding: 6px 10px;
		font-size: 12px;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.06);
		color: inherit;
		cursor: pointer;
	}

	button.ghost {
		background: transparent;
	}

	code {
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 12px;
	}

	.safeModeToggle {
		display: flex;
		align-items: center;
		gap: 8px;
		margin-top: 6px;
		font-size: 12px;
	}
</style>
