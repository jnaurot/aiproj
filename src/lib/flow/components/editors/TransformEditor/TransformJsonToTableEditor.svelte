<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformJsonToTableParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformJsonToTableParams>;
	export let onDraft: (patch: Partial<TransformJsonToTableParams>) => void;
	export let onCommit: (patch: Partial<TransformJsonToTableParams>) => void;

	$: void selectedNode?.id;
	$: orient = params?.orient ?? 'records';
	$: rowsKey = params?.rowsKey ?? 'rows';
</script>

<Section title="JSON To Table">
	<div class="hint">Convert JSON payloads into a table.</div>

	<Field label="orient">
		<select
			value={orient}
			on:change={(event) => {
				const next = (event.currentTarget as HTMLSelectElement).value as TransformJsonToTableParams['orient'];
				onDraft({ orient: next });
				onCommit({ orient: next });
			}}
		>
			<option value="records">records</option>
			<option value="object">object</option>
		</select>
	</Field>

	<Field label="rowsKey">
		<Input
			value={rowsKey}
			placeholder="rows"
			onInput={(event) => onDraft({ rowsKey: (event.currentTarget as HTMLInputElement).value })}
			onBlur={() => onCommit({ rowsKey: rowsKey.trim() || 'rows' })}
		/>
	</Field>
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}
</style>
