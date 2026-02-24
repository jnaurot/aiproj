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
# df is the input pandas DataFrame (DuckDB â†’ pandas conversion happens automatically)
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
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 12px;
		padding: 12px;
		background: rgba(255, 255, 255, 0.03);
		margin-top: 8px;
	}

	.sectionTitle {
		font-weight: 650;
		font-size: 14px;
		margin-bottom: 10px;
		opacity: 0.9;
	}

	.subTitle {
		margin-top: 10px;
		font-weight: 600;
		font-size: 13px;
		opacity: 0.95;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-bottom: 10px;
		line-height: 1.35;
	}

	.row {
		display: flex;
		gap: 8px;
		align-items: flex-start;
		margin-bottom: 8px;
	}

	.line {
		display: flex;
		gap: 8px;
		align-items: center;
		margin-bottom: 8px;
	}

	.field {
		display: grid;
		grid-template-columns: 100px minmax(0, 1fr);
		align-items: start;
		gap: 8px;
		margin-bottom: 10px;
	}

	.field.grow {
		flex: 1;
	}

	.field.dir {
		grid-template-columns: 70px minmax(0, 1fr);
	}

	.k,
	.label {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
		font-weight: 400;
	}

	.v {
		min-width: 0;
		width: 100%;
	}

	.colInput {
		flex: 1;
	}

	.arrow {
		opacity: 0.75;
		padding-top: 8px;
	}

	.toggle {
		display: inline-flex;
		gap: 8px;
		align-items: center;
	}

	input,
	select,
	textarea,
	.readonly,
	.code {
		width: 100%;
		box-sizing: border-box;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(0, 0, 0, 0.2);
		color: inherit;
		padding: 8px 10px;
		font-size: 14px;
		outline: none;
		min-height: 40px;
	}

	textarea,
	.code {
		resize: vertical;
		line-height: 1.35;
		min-height: 96px;
	}

	input[type='checkbox'] {
		width: auto;
		min-height: 0;
		padding: 0;
	}

	input:focus,
	select:focus,
	textarea:focus,
	.code:focus,
	.readonly:focus {
		border-color: rgba(255, 255, 255, 0.25);
	}

	.actions,
	.snips {
		margin-top: 8px;
		display: flex;
		gap: 8px;
		justify-content: flex-end;
		flex-wrap: wrap;
	}

	.snipsTitle {
		font-size: 12px;
		opacity: 0.8;
		align-self: center;
	}

	.snipRow {
		display: flex;
		gap: 8px;
		width: 100%;
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

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
		color: #fecaca;
	}

	.warn {
		margin-top: 8px;
		font-size: 12px;
		color: #fca5a5;
		white-space: pre-wrap;
	}

	.warn ul {
		margin: 6px 0 0 16px;
		padding: 0;
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
		opacity: 0.95;
	}

	code {
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
		font-size: 12px;
	}
</style>

