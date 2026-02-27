<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformFilterParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformFilterParams>;
	export let onDraft: (patch: Partial<TransformFilterParams>) => void;
	export let onCommit: (patch: Partial<TransformFilterParams>) => void;

	$: void selectedNode?.id;
	$: expr = typeof params?.expr === 'string' ? params.expr : 'length(text) > 10';
</script>

<Section title="Filter">
	<Field label="expr">
		<Input
			value={expr}
			placeholder="e.g. length(text) > 10"
			onInput={(event) => onDraft({ expr: (event.currentTarget as HTMLInputElement).value })}
			onBlur={(event) => onCommit({ expr: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>
</Section>
