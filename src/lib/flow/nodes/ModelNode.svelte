<script lang="ts">
	import type { LlmNodeData, ModelNodeData } from '$lib/flow/types';
	import BaseNode from './BaseNode.svelte';
	import { modelNodeMeta } from './modelNodeMeta';
	import { graphStore } from '$lib/flow/store/graphStore';
	import { statusProjectionFromBinding } from '$lib/flow/store/runScope';
	import { reconcileLifecycleForActiveRun, reconcileModelLeaseLifecycle } from '$lib/flow/store/statusModel';

	export let id: string;
	export let selected: boolean = false;
	export let data: LlmNodeData | ModelNodeData;

	$: meta = modelNodeMeta(data);
	$: binding = ($graphStore as any)?.nodeBindings?.[id];
	$: statusProjection = statusProjectionFromBinding(binding as any);
	$: runStatus = String(($graphStore as any)?.runStatus ?? 'idle');
	$: schedulerRows = Array.isArray(($graphStore as any)?.queueRuntime?.schedulerSnapshot?.perNode)
		? (($graphStore as any).queueRuntime.schedulerSnapshot.perNode as Array<Record<string, unknown>>)
		: [];
	$: schedulerRow = schedulerRows.find((row) => String(row?.nodeId ?? '') === String(id)) ?? {};
	$: pendingInputCount = Math.max(0, Number((schedulerRow as any)?.pendingInputCount ?? 0));
	$: inflightCount = Math.max(0, Number((schedulerRow as any)?.inflight ?? 0));
	$: readyWork = Boolean((schedulerRow as any)?.readyWork ?? false);
	$: blockedReasonCode = String((schedulerRow as any)?.lastBlockedReasonCode ?? '').trim();
	$: lifecycle = reconcileModelLeaseLifecycle({
		lifecycle: reconcileLifecycleForActiveRun({
			lifecycle: statusProjection.lifecycle,
			consumeMode: 'single_item',
			runStatus,
			inflight: inflightCount,
			pendingInputCount,
			readyWork,
			blockedReasonCode
		}),
		nodeKind: String((data as any)?.kind ?? 'model'),
		hasActiveLeaseStar: Boolean((data as any)?.meta?.llmAllocated),
		runStatus
	});
	$: llmAllocated = lifecycle === 'running' && Boolean((data as any)?.meta?.llmAllocated);
	$: workHandleStats = (() => {
		const byHandle =
			(($graphStore as any)?.queueRuntime?.runScoped?.runtimeItemMetrics?.byHandle ??
				($graphStore as any)?.queueRuntime?.runtimeItemMetrics?.byHandle ??
				{}) as Record<string, any>;
		let accepted = 0;
		let rejected = 0;
		for (const metric of Object.values(byHandle)) {
			if (!metric || typeof metric !== 'object') continue;
			if (String((metric as any).nodeId ?? '') !== String(id)) continue;
			const plane = String((metric as any).plane ?? 'work').trim().toLowerCase();
			if (plane !== 'work') continue;
			accepted += Number((metric as any).itemsAccepted ?? 0);
			rejected += Number((metric as any).itemsRejected ?? 0);
		}
		return {
			accepted: Math.max(0, accepted),
			rejected: Math.max(0, rejected),
			total: Math.max(0, accepted + rejected)
		};
	})();
</script>

<BaseNode {id} {data} {selected}>
	<div style="font-size:12px; opacity:0.85; display:flex; align-items:center; gap:4px;">
		<span>Model:</span>
		{#if llmAllocated}
			<span style="color:#facc15; line-height:1;">&#9733;</span>
		{/if}
		<span>{meta.model}</span>
	</div>
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
	<svelte:fragment slot="footer-right">
		{#if workHandleStats.total > 0}
			<span style="font-size:12px;">
				ok: {workHandleStats.accepted}/{workHandleStats.total}
				{#if workHandleStats.rejected > 0}
					<span style="opacity:0.75;"> (skipped {workHandleStats.rejected})</span>
				{/if}
			</span>
		{/if}
	</svelte:fragment>
</BaseNode>
