<script lang="ts">
	import type { TransformNodeData } from '$lib/flow/types';
	import BaseNode from './BaseNode.svelte';

	export let data: TransformNodeData;
	export let id: string;
	export let selected: boolean = false;

	function summary(op: TransformNodeData['params']['op'], params: Record<string, any>): string {
		if (op === 'filter') return params.filter?.expr ?? '-';
		if (op === 'select') return (params.select?.columns || []).join(', ') || '-';
		if (op === 'rename') return Object.keys(params.rename?.map || {}).length > 0 ? 'Rename columns' : '-';
		if (op === 'derive') return (params.derive?.columns || []).length > 0 ? 'Derive columns' : '-';
		if (op === 'aggregate') return (params.aggregate?.metrics || []).length > 0 ? 'Aggregate' : '-';
		if (op === 'join') return (params.join?.clauses || []).length > 0 ? 'Join' : '-';
		if (op === 'sort') return (params.sort?.by || []).length > 0 ? 'Sort' : '-';
		if (op === 'limit') return params.limit?.n ? `Limit ${params.limit.n}` : '-';
		if (op === 'dedupe') return 'Deduplicate';
		if (op === 'quality_gate') return (params.quality_gate?.checks || []).length > 0 ? 'Quality gate' : '-';
		if (op === 'sql') return params.sql?.query ? 'SQL Query' : '-';
		return '-';
	}

	$: label = summary(data?.params?.op ?? 'filter', data?.params ?? {});
</script>

<BaseNode {id} {data} {selected}>
	<div style="font-size:12px; opacity:0.85;">
		{label}
	</div>
</BaseNode>
