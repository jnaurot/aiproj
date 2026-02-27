<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformLimitParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { parseOptionalInt } from '$lib/flow/components/editors/shared';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformLimitParams>;
	export let onDraft: (patch: Partial<TransformLimitParams>) => void;
	export let onCommit: (patch: Partial<TransformLimitParams>) => void;

	const defaultValue = 100;

	$: void selectedNode?.id;
	$: n = params?.n ?? defaultValue;
</script>

<Section title="Limit">
	<div class="hint">Return only the first <code>n</code> rows.</div>

	<Field label="n">
		<div class="row">
			<Input
				type="number"
				min="1"
				step="1"
				value={n}
				onInput={(event) => onDraft({ n: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) ?? defaultValue })}
				onBlur={() => onCommit({ n: n < 1 ? 1 : n })}
			/>
			<button
				class="small ghost"
				type="button"
				on:click={() => {
					onDraft({ n: defaultValue });
					onCommit({ n: defaultValue });
				}}
			>
				Reset
			</button>
		</div>
	</Field>

	<div class="preview">
		<div class="subTitle">Preview</div>
		<pre>SELECT * FROM input LIMIT {n < 1 ? 1 : n}</pre>
	</div>
</Section>

<style>
	.row {
		display: flex;
		gap: 8px;
		align-items: center;
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
