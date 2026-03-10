<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformTextToTableParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformTextToTableParams>;
	export let onDraft: (patch: Partial<TransformTextToTableParams>) => void;
	export let onCommit: (patch: Partial<TransformTextToTableParams>) => void;

	$: void selectedNode?.id;
	$: mode = params?.mode ?? 'lines';
	$: column = params?.column ?? 'text';
	$: delimiter = params?.delimiter ?? ',';
	$: hasHeader = params?.hasHeader ?? true;
</script>

<Section title="Text To Table">
	<div class="hint">Parse text into rows and columns.</div>

	<Field label="mode">
		<select
			value={mode}
			on:change={(event) => {
				const next = (event.currentTarget as HTMLSelectElement).value as TransformTextToTableParams['mode'];
				onDraft({ mode: next });
				onCommit({ mode: next });
			}}
		>
			<option value="lines">lines</option>
			<option value="csv">csv</option>
			<option value="tsv">tsv</option>
		</select>
	</Field>

	<Field label="column">
		<Input
			value={column}
			placeholder="text"
			onInput={(event) => onDraft({ column: (event.currentTarget as HTMLInputElement).value })}
			onBlur={() => onCommit({ column: column.trim() || 'text' })}
		/>
	</Field>

	<Field label="delimiter">
		<Input
			value={delimiter}
			placeholder=","
			onInput={(event) => onDraft({ delimiter: (event.currentTarget as HTMLInputElement).value })}
			onBlur={() => onCommit({ delimiter: delimiter || ',' })}
		/>
	</Field>

	<Field label="hasHeader">
		<label class="check">
			<input
				type="checkbox"
				checked={hasHeader}
				on:change={(event) => {
					const next = (event.currentTarget as HTMLInputElement).checked;
					onDraft({ hasHeader: next });
					onCommit({ hasHeader: next });
				}}
			/>
			<span>First row is header (csv/tsv)</span>
		</label>
	</Field>
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	.check {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 13px;
	}
</style>
