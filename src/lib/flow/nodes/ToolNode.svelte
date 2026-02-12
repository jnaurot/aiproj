<script lang="ts">
	import type { ToolNodeData } from '$lib/flow/types';
	import BaseNode from './BaseNode.svelte';

	export let id: string;
	export let selected: boolean = false;
	export let data: ToolNodeData;

	$: p = (data?.params ?? {}) as any;
	$: provider = p.provider ?? "unknown";
	$: name =
		provider === "mcp"
			? (p.mcp?.toolName ?? "—")
			: (p.name ?? "—");
</script>

<BaseNode {id} {data} {selected}>
	<div style="font-size:12px; opacity:0.85;">
		Tool: {data.params.provider} / {data.params.provider === 'mcp'
			? data.params.mcp.toolName
			: data.params.name}
	</div>
</BaseNode>
