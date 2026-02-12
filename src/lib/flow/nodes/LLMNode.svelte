<script lang="ts">
	import type { LlmNodeData } from '$lib/flow/types';
	import BaseNode from './BaseNode.svelte';

	export let id: string;
	export let selected: boolean = false;
	export let data: LlmNodeData;
</script>

<BaseNode {id} {data} {selected}>
	<div style="font-size:12px; opacity:0.85;">
		Model: {String(data?.params?.model ?? '—')}
	</div>
</BaseNode>
