<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformSqlParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type Dialect = NonNullable<TransformSqlParams['dialect']>;

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformSqlParams>;
	export let onDraft: (patch: Partial<TransformSqlParams>) => void;
	export let onCommit: (patch: Partial<TransformSqlParams>) => void;

	const defaults: TransformSqlParams = {
		dialect: 'duckdb',
		query: 'SELECT * FROM input LIMIT 10'
	};

	$: void selectedNode?.id;
	$: dialect = params?.dialect ?? defaults.dialect;
	$: query = typeof params?.query === 'string' ? params.query : defaults.query;

	function insertSnippet(snippet: string): void {
		const merged = query.trimEnd().length > 0 ? `${query.trimEnd()}\n\n${snippet}` : snippet;
		onDraft({ query: merged });
		onCommit({ query: merged });
	}
</script>

<Section title="SQL Query">
	<div class="hint">Write SQL against <code>input</code>.</div>

	<Field label="dialect">
		<select
			value={dialect}
			on:change={(event) => {
				const value = (event.currentTarget as HTMLSelectElement).value as Dialect;
				onDraft({ dialect: value });
				onCommit({ dialect: value });
			}}
		>
			<option value="duckdb">duckdb</option>
			<option value="postgres">postgres</option>
			<option value="sqlite">sqlite</option>
		</select>
	</Field>

	<Field label="query">
		<div class="stack">
			<Input
				multiline={true}
				rows={10}
				value={query}
				placeholder={defaults.query}
				onInput={(event) => onDraft({ query: (event.currentTarget as HTMLTextAreaElement).value })}
				onBlur={() => onCommit({ query: query.trim() })}
			/>
			<div class="actions">
				<button
					class="small ghost"
					type="button"
					on:click={() => {
						onDraft(defaults);
						onCommit(defaults);
					}}
				>
					Reset
				</button>
				<button class="small" type="button" on:click={() => onCommit({ query: query.trim() })}>Commit</button>
			</div>
		</div>
	</Field>

	<div class="actions">
		<button class="small" type="button" on:click={() => insertSnippet('SELECT * FROM input LIMIT 10;')}>Limit</button>
		<button class="small" type="button" on:click={() => insertSnippet('SELECT COUNT(*) AS cnt FROM input;')}>Count</button>
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
</Section>

<style>
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
</style>
