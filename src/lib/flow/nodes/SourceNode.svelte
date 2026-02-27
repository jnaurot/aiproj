<script lang="ts">
	import type { SourceNodeData } from '$lib/flow/types';
	import BaseNode from './BaseNode.svelte';

	export let data: SourceNodeData;
	export let id: string;
	export let selected: boolean = false;

	function summary(sourceKind: SourceNodeData['sourceKind'], params: Record<string, any>): string {
		if (sourceKind === 'file') {
			const sid = typeof params.snapshotId === 'string' ? String(params.snapshotId).toLowerCase() : '';
			const currentMetaName =
				typeof params?.snapshotMetadata?.originalFilename === 'string'
					? String(params.snapshotMetadata.originalFilename)
					: '';
			if (currentMetaName) return currentMetaName;
			if (Array.isArray(params.recentSnapshots) && sid) {
				const hit = params.recentSnapshots.find((s: any) => String(s?.id ?? '').toLowerCase() === sid);
				if (typeof hit?.filename === 'string' && hit.filename) return String(hit.filename);
			}
			if (typeof params.snapshotId === 'string' && params.snapshotId) {
				return `snapshot:${String(params.snapshotId).slice(0, 8)}`;
			}
			return params.filename ?? '-';
		}
		if (sourceKind === 'api') return params.url ?? '-';
		return params.query ?? params.table_name ?? '-';
	}

	$: label = summary(data.sourceKind ?? 'file', data.params ?? {});
</script>

<BaseNode {id} {data} {selected}>
	<div style="font-size:12px; opacity:0.85;">
		Source: {label}
	</div>
</BaseNode>
