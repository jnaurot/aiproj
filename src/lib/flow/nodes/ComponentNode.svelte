<script lang="ts">
	import type { ComponentNodeData } from '$lib/flow/types';
	import BaseNode from './BaseNode.svelte';

	export let id: string;
	export let selected: boolean = false;
	export let data: ComponentNodeData;

	$: componentRef = (data?.params?.componentRef ?? {}) as {
		componentId?: string;
		revisionId?: string;
	};
	$: componentId = String(componentRef.componentId ?? '').trim() || 'unselected';
	$: revisionId = String(componentRef.revisionId ?? '').trim() || '-';
	$: revisionDisplay =
		revisionId === '-'
			? '-'
			: revisionId.length > 15
				? `${revisionId.slice(0, 15)}...`
				: revisionId;
	$: api = (data?.params?.api ?? {}) as { inputs?: unknown[]; outputs?: unknown[] };
	$: inputCount = Array.isArray(api.inputs) ? api.inputs.length : 0;
	$: outputCount = Array.isArray(api.outputs) ? api.outputs.length : 0;
	$: outputHandles =
		Array.isArray(api.outputs) && api.outputs.length > 0
			? api.outputs
					.map((out, index) => {
						const name = String((out as any)?.name ?? '').trim();
						const effectiveName = name || (index === 0 ? 'default' : `out_${index + 1}`);
						return { id: effectiveName, label: effectiveName };
					})
					.filter((v): v is { id: string; label: string } => Boolean(v))
			: null;
</script>

<BaseNode {id} {data} {selected} sourceHandles={outputHandles}>
	<div style="font-size:12px; opacity:0.9;">
		Component: {componentId}
	</div>
	<div style="font-size:11px; opacity:0.75; margin-top:2px;" title={revisionId}>
		rev {revisionDisplay}
	</div>
	<div style="font-size:11px; opacity:0.75; margin-top:2px;">
		api in {inputCount} / out {outputCount}
	</div>
</BaseNode>
