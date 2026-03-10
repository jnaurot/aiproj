<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformTableToJsonParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformTableToJsonParams>;
	export let onDraft: (patch: Partial<TransformTableToJsonParams>) => void;
	export let onCommit: (patch: Partial<TransformTableToJsonParams>) => void;

	$: void selectedNode?.id;
	$: orient = params?.orient ?? 'records';
	$: pretty = params?.pretty ?? false;
</script>

<Section title="Table To JSON">
	<div class="hint">Emit JSON output for downstream tools, LLMs, or APIs.</div>

	<Field label="orient">
		<select
			value={orient}
			on:change={(event) => {
				const next = (event.currentTarget as HTMLSelectElement).value as TransformTableToJsonParams['orient'];
				onDraft({ orient: next });
				onCommit({ orient: next });
			}}
		>
			<option value="records">records</option>
			<option value="split">split</option>
		</select>
	</Field>

	<Field label="pretty">
		<label class="check">
			<input
				type="checkbox"
				checked={pretty}
				on:change={(event) => {
					const next = (event.currentTarget as HTMLInputElement).checked;
					onDraft({ pretty: next });
					onCommit({ pretty: next });
				}}
			/>
			<span>Pretty-print output JSON</span>
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
