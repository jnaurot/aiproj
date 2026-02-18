<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData } from '$lib/flow/types/transform';
	import type { TransformSqlParams } from '$lib/flow/schema/transform';

	// NOTE: ports/enabled/notes handled upstream. This editor only edits sql params.
	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformSqlParams;
	export let onDraft: (patch: Partial<TransformSqlParams>) => void;
	export let onCommit: (patch: Partial<TransformSqlParams>) => void;

	const DEFAULTS: TransformSqlParams = {
		dialect: 'duckdb',
		query: 'SELECT * FROM input LIMIT 10'
	};

	$: dialect = (params?.dialect ?? DEFAULTS.dialect) as TransformSqlParams['dialect'];
	$: query = (params?.query ?? DEFAULTS.query) as string;

	function setDialect(next: TransformSqlParams['dialect']) {
		onDraft({ dialect: next });
		onCommit({ dialect: next });
	}

	function setQuery(next: string) {
		onDraft({ query: next });
	}

	function commitQuery() {
		onCommit({ query: (query ?? '').trim() });
	}

	function resetDefaults() {
		onDraft({ ...DEFAULTS });
		onCommit({ ...DEFAULTS });
	}

	function insertSnippet(snippet: string) {
		const next = (query ?? '').trimEnd();
		const sep = next.length ? '\n\n' : '';
		const merged = `${next}${sep}${snippet}`;
		onDraft({ query: merged });
		onCommit({ query: merged });
	}
</script>

<div class="section">
	<div class="sectionTitle">SQL Query</div>

	<div class="hint">
		Write SQL against <code>input</code>. (Join support can be added later as <code>other</code>, etc.)
	</div>

	<div class="field">
		<div class="k">dialect</div>
		<div class="v">
			<select value={dialect} on:change={(e) => setDialect((e.currentTarget as HTMLSelectElement).value as any)}>
				<option value="duckdb">duckdb</option>
				<option value="postgres">postgres</option>
				<option value="sqlite">sqlite</option>
			</select>
		</div>
	</div>

	<div class="field">
		<div class="k">query</div>
		<div class="v">
			<textarea
				class="code"
				rows={10}
				placeholder={DEFAULTS.query}
				value={query}
				on:input={(e) => setQuery((e.currentTarget as HTMLTextAreaElement).value)}
				on:blur={commitQuery}
			/>
			<div class="actions">
				<button class="small ghost" on:click={resetDefaults}>Reset</button>
				<button class="small" on:click={commitQuery}>Commit</button>
			</div>
		</div>
	</div>

	<div class="snips">
		<div class="snipsTitle">Snippets</div>
		<div class="snipRow">
			<button class="small" on:click={() => insertSnippet('SELECT * FROM input LIMIT 10;')}>Limit</button>
			<button class="small" on:click={() => insertSnippet('SELECT COUNT(*) AS cnt FROM input;')}>Count</button>
			<button
				class="small"
				on:click={() =>
					insertSnippet('SELECT *\nFROM input\nWHERE length(text) > 10\nORDER BY text ASC\nLIMIT 50;')}
			>
				Filter + sort
			</button>
		</div>
	</div>

	<div class="preview">
		<div class="label">Preview</div>
		<pre>{query}</pre>
	</div>
</div>

<style>
	.section {
		margin-top: 8px;
	}

	.sectionTitle {
		font-weight: 600;
		margin-bottom: 6px;
	}

	.hint {
		font-size: 12px;
		opacity: 0.8;
		margin-bottom: 10px;
	}

	.field {
		display: grid;
		grid-template-columns: 110px 1fr;
		gap: 10px;
		align-items: start;
		margin-bottom: 10px;
	}

	.k {
		font-size: 12px;
		opacity: 0.85;
	}

	.v select {
		width: 100%;
		padding: 4px 6px;
		border-radius: 4px;
		border: 1px solid #ccc;
		box-sizing: border-box;
	}

	.code {
		width: 100%;
		padding: 8px;
		border-radius: 6px;
		border: 1px solid #ccc;
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New',
			monospace;
		font-size: 12px;
		box-sizing: border-box;
	}

	.actions {
		margin-top: 6px;
		display: flex;
		gap: 8px;
		justify-content: flex-end;
	}

	button.small {
		padding: 4px 8px;
		font-size: 12px;
		border-radius: 4px;
		border: 1px solid #ccc;
		background: #fff;
		cursor: pointer;
	}

	button.ghost {
		background: transparent;
	}

	.snips {
		margin-top: 10px;
		padding-top: 8px;
		border-top: 1px solid #ddd;
	}

	.snipsTitle {
		font-weight: 600;
		font-size: 12px;
		margin-bottom: 6px;
		opacity: 0.9;
	}

	.snipRow {
		display: flex;
		gap: 8px;
		flex-wrap: wrap;
	}

	.preview {
		margin-top: 12px;
	}

	.label {
		font-weight: 700;
		margin-bottom: 6px;
	}

	pre {
		white-space: pre-wrap;
		word-break: break-word;
		padding: 8px;
		border: 1px solid #ddd;
		border-radius: 6px;
		font-size: 12px;
		opacity: 0.95;
	}
</style>
