<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformPythonParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformPythonParams>;
	export let onDraft: (patch: Partial<TransformPythonParams>) => void;
	export let onCommit: (patch: Partial<TransformPythonParams>) => void;

	const defaults: TransformPythonParams = {
		language: 'python',
		source: `# Python transform example
# df is the input pandas DataFrame

def transform(df):
    df['length'] = df['text'].str.len()
    return df[df['length'] > 20]
`
	};

	$: void selectedNode?.id;
	$: source = typeof params?.source === 'string' ? params.source : defaults.source;

	function insertSnippet(snippet: string): void {
		const merged = source.trimEnd().length > 0 ? `${source.trimEnd()}\n\n${snippet}` : snippet;
		onDraft({ source: merged });
		onCommit({ source: merged, language: 'python' });
	}
</script>

<Section title="Python Code">
	<div class="hint">
		This op is typically <b>disabled in the backend</b> unless explicitly enabled.
	</div>

	<Field label="language">
		<Input value="python" readonly={true} />
	</Field>

	<Field label="source">
		<div class="stack">
			<Input
				multiline={true}
				rows={14}
				value={source}
				placeholder={defaults.source}
				onInput={(event) => onDraft({ source: (event.currentTarget as HTMLTextAreaElement).value })}
				onBlur={() => onCommit({ source, language: 'python' })}
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
				<button class="small" type="button" on:click={() => onCommit({ source, language: 'python' })}>Commit</button>
			</div>
		</div>
	</Field>

	<div class="actions">
		<button
			class="small"
			type="button"
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
			type="button"
			on:click={() =>
				insertSnippet(`# select columns
def transform(df):
    cols = [c for c in ['id', 'text'] if c in df.columns]
    return df[cols]
`)}
		>
			Select columns
		</button>
	</div>

	<div class="preview">
		<div class="subTitle">Preview</div>
		<pre>{source}</pre>
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

</style>
