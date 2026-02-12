<script lang="ts">
	import type { SourceNodeData } from '$lib/flow/types';
	import BaseNode from './BaseNode.svelte';

	export let data: SourceNodeData;
	export let id: string;
	export let selected: boolean = false;

	function summary(sourceKind: SourceNodeData['sourceKind'], params: Record<string, any>): string {
		if (sourceKind === 'file') return params.file_path ?? params.file_name ?? '—';
		if (sourceKind === 'api') return params.url ?? '—';
		// database
		return params.query ?? params.table_name ?? '—';
	}

	$: label = summary(data.sourceKind ?? 'file', data.params ?? {});
</script>

<BaseNode {id} {data} {selected}>
	<div style="font-size:12px; opacity:0.85;">
		Source: {label}
	</div>
</BaseNode>
