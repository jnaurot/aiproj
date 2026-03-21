<script lang="ts">
	import type { LlmNodeData, ModelNodeData } from '$lib/flow/types';
	import BaseNode from './BaseNode.svelte';
	import { modelNodeMeta } from './modelNodeMeta';

	export let id: string;
	export let selected: boolean = false;
	export let data: LlmNodeData | ModelNodeData;

	$: meta = modelNodeMeta(data);
</script>

<BaseNode {id} {data} {selected}>
	<div style="font-size:12px; opacity:0.85;">Model: {meta.model}</div>
	<div style="display:flex; flex-wrap:wrap; gap:6px; margin-top:8px;">
		<span style="font-size:11px; border:1px solid rgba(255,255,255,0.15); border-radius:999px; padding:2px 8px;"
			>{meta.modelKind}</span
		>
		<span style="font-size:11px; border:1px solid rgba(255,255,255,0.15); border-radius:999px; padding:2px 8px;"
			>{meta.taskKind}</span
		>
		<span style="font-size:11px; border:1px solid rgba(255,255,255,0.15); border-radius:999px; padding:2px 8px;"
			>{meta.provider}</span
		>
		<span style="font-size:11px; border:1px solid rgba(255,255,255,0.15); border-radius:999px; padding:2px 8px;"
			>{meta.outputMode}</span
		>
	</div>
</BaseNode>
