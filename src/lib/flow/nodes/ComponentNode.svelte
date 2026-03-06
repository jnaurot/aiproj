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
	$: api = (data?.params?.api ?? {}) as { inputs?: unknown[]; outputs?: unknown[] };
	$: inputCount = Array.isArray(api.inputs) ? api.inputs.length : 0;
	$: outputCount = Array.isArray(api.outputs) ? api.outputs.length : 0;
</script>

<BaseNode {id} {data} {selected}>
	<div style="font-size:12px; opacity:0.9;">
		Component: {componentId}
	</div>
	<div style="font-size:11px; opacity:0.75; margin-top:2px;">
		rev {revisionId} | api in {inputCount} / out {outputCount}
	</div>
</BaseNode>
