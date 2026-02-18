<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData } from '$lib/flow/types/transform';
	import type { TransformPythonParams } from '$lib/flow/schema/transform';

	// NOTE: ports/enabled/notes handled upstream. This editor only edits python params.
	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformPythonParams;
	export let onDraft: (patch: Partial<TransformPythonParams>) => void;
	export let onCommit: (patch: Partial<TransformPythonParams>) => void;

	const DEFAULTS: TransformPythonParams = {
		language: 'python',
		source: `# Python transform example
# df is the input pandas DataFrame (DuckDB → pandas conversion happens automatically)
# return a pandas DataFrame

def transform(df):
    df['length'] = df['text'].str.len()
    return df[df['length'] > 20]
`
	};

	$: language = (params?.language ?? DEFAULTS.language) as TransformPythonParams['language'];
	$: source = (params?.source ?? DEFAULTS.source) as string;

	function setSource(next: string) {
		onDraft({ source: next });
	}

	function commitSource() {
		onCommit({
			language: 'python',
			source: source ?? ''
		});
	}

	function resetDefaults() {
		onDraft({ ...DEFAULTS });
		onCommit({ ...DEFAULTS });
	}

	function insertSnippet(snippet: string) {
		const next = (source ?? '').trimEnd();
		const sep = next.length ? '\n\n' : '';
		const merged = `${next}${sep}${snippet}`;
		onDraft({ source: merged });
		onCommit({ source: merged, language: 'python' });
	}
</script>

<div class="section">
	<div class="sectionTitle">Python Code</div>

	<div class="hint">
		This op is typically <b>disabled in the backend</b> for determinism unless explicitly enabled.
		The function signature should be <code>def transform(df):</code> and return a DataFrame.
	</div>

	<div class="field">
		<div class="k">language</div>
		<div class="v">
			<!-- Keep fixed to python to avoid shape drift -->
			<input class="readonly" type="text" value={language} readonly />
		</div>
	</div>

	<div class="field">
		<div class="k">source</div>
		<div class="v">
			<textarea
				class="code"
				rows={14}
				placeholder={DEFAULTS.source}
				value={source}
				on:input={(e) => setSource((e.currentTarget as HTMLTextAreaElement).value)}
				on:blur={commitSource}
			/>
			<div class="actions">
				<button class="small ghost" on:click={resetDefaults}>Reset</button>
				<button class="small" on:click={commitSource}>Commit</button>
			</div>
		</div>
	</div>

	<div class="snips">
		<div class="snipsTitle">Snippets</div>
		<div class="snipRow">
			<button
				class="small"
				on:click={() =>
					insertSnippet(`# keep only rows where text length > 20
def transform(df):
    df['length'] = df['text'].astype(str).str.len()
    return df[df['length'] > 20]
`)}
			>
				Filter by length
			</button>

			<button
				class="small"
				on:click={() =>
					insertSnippet(`# select columns (safe if missing handled upstream)
def transform(df):
    cols = [c for c in ['id', 'text'] if c in df.columns]
    return df[cols]
`)}
			>
				Select columns
			</button>

			<button
				class="small"
				on:click={() =>
					insertSnippet(`# add derived column
def transform(df):
    df = df.copy()
    if 'text' in df.columns:
        df['upper_text'] = df['text'].astype(str).str.upper()
    return df
`)}
			>
				Derive column
			</button>
		</div>
	</div>

	<div class="preview">
		<div class="label">Preview</div>
		<pre>{source}</pre>
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

	.readonly {
		width: 100%;
		padding: 6px 8px;
		border-radius: 4px;
		border: 1px solid #ccc;
		box-sizing: border-box;
		opacity: 0.85;
		background: #f7f7f7;
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
