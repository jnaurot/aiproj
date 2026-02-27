<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformParams, TransformKind } from '$lib/flow/schema/transform';
	import { defaultTransformParamsByKind } from '$lib/flow/schema/transformDefaults';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { TransformEditorByKind } from './TransformEditor';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformParams>;
	export let onDraft: (patch: Partial<TransformParams>) => void;
	export let onCommit: (patch: Partial<TransformParams>) => void;

	const ops: TransformKind[] = ['filter', 'select', 'rename', 'derive', 'aggregate', 'join', 'sort', 'limit', 'dedupe', 'sql'];

	$: void selectedNode?.id;
	$: currentOp = params?.op ?? 'filter';
	$: enabled = params?.enabled ?? true;
	$: notes = params?.notes ?? '';
	$: EditorComponent = TransformEditorByKind[currentOp];
	$: childParams = (params as Record<string, unknown>)[currentOp] ?? defaultTransformParamsByKind[currentOp][currentOp];

	function handleOpChange(nextOp: TransformKind): void {
		const next = structuredClone(defaultTransformParamsByKind[nextOp]);
		onDraft(next);
		onCommit(next);
	}

	function patchChild(next: Record<string, unknown>): void {
		onDraft({ [currentOp]: next } as Partial<TransformParams>);
	}

	function commitChild(next: Record<string, unknown>): void {
		onCommit({ [currentOp]: next } as Partial<TransformParams>);
	}
</script>

<Section title="Transform">
	<Field label="op">
		<select
			value={currentOp}
			on:change={(event) => handleOpChange((event.currentTarget as HTMLSelectElement).value as TransformKind)}
		>
			{#each ops as op}
				<option value={op}>{op}</option>
			{/each}
		</select>
	</Field>

	<Field label="enabled">
		<Input
			type="checkbox"
			checked={enabled}
			onChange={(event) => onDraft({ enabled: (event.currentTarget as HTMLInputElement).checked })}
		/>
	</Field>

	<Field label="notes">
		<Input
			multiline={true}
			rows={3}
			value={notes}
			onInput={(event) => onDraft({ notes: (event.currentTarget as HTMLTextAreaElement).value })}
		/>
	</Field>
</Section>

{#if EditorComponent}
	<svelte:component
		this={EditorComponent}
		{selectedNode}
		params={childParams}
		onDraft={patchChild}
		onCommit={commitChild}
	/>
{/if}
