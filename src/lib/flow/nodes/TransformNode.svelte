<script lang="ts">
	import type { TransformNodeData } from '$lib/flow/types';
	import BaseNode from './BaseNode.svelte';
  
	export let data: TransformNodeData;
	export let id: string;
	export let selected: boolean = false;
</script>

<BaseNode {id} {data} {selected}>
	<div style="font-size:12px; opacity:0.85;">
		Op: {String(data?.params?.op ?? '—')}
	</div>
</BaseNode>
