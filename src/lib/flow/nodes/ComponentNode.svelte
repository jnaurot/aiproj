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
	$: exposureRegistry = Array.isArray((data as any)?.params?.exposureRegistry)
		? ((data as any).params.exposureRegistry as Array<Record<string, any>>)
		: [];
	$: outputSourceByAlias = Object.fromEntries(
		exposureRegistry
			.filter((rec) => String(rec?.kind ?? '').trim().toLowerCase() === 'data_output')
			.map((rec) => [String(rec?.alias ?? '').trim(), String(rec?.internal_source_path ?? '').trim()])
			.filter(([alias]) => String(alias ?? '').trim().length > 0)
	) as Record<string, string>;
	$: inputCount = Array.isArray(api.inputs) ? api.inputs.length : 0;
	$: outputCount = Array.isArray(api.outputs) ? api.outputs.length : 0;
	$: checkpointSummary = (((data as any)?.meta ?? {}) as any)?.checkpointSummary ?? null;
	$: checkpointTotal = Math.max(0, Number((checkpointSummary as any)?.total ?? 0));
	$: checkpointValid = Math.max(0, Number((checkpointSummary as any)?.valid ?? 0));
	$: checkpointStale = Math.max(0, Number((checkpointSummary as any)?.stale ?? 0));
	$: checkpointBadgeTone = checkpointStale > 0 ? 'checkpointBadgeStale' : 'checkpointBadgeValid';
	$: checkpointBadgeText =
		checkpointStale > 0
			? `ckpt ${checkpointTotal} (${checkpointStale} stale)`
			: `ckpt ${checkpointTotal} (${checkpointValid} valid)`;
	function internalNodeNameFromOutputRef(outputRef: string): string {
		const raw = String(outputRef ?? '').trim();
		if (!raw) return '';
		const base = raw.includes('|') ? raw.split('|')[0] : raw;
		const afterKind = base.includes(':') ? base.split(':').slice(1).join(':') : base;
		const trimmed = String(afterKind ?? '').trim();
		if (!trimmed) return '';
		if (!trimmed.includes('/')) return trimmed;
		const parts = trimmed
			.split('/')
			.map((part) => String(part ?? '').trim())
			.filter((part) => part.length > 0);
		return parts[parts.length - 1] ?? trimmed;
	}
	$: outputHandles =
		Array.isArray(api.outputs) && api.outputs.length > 0
			? api.outputs
					.map((out, index) => {
						const name = String((out as any)?.name ?? '').trim();
						const effectiveName = name || (index === 0 ? 'default' : `out_${index + 1}`);
						const outputRef = String(outputSourceByAlias?.[effectiveName] ?? '').trim();
						const internalName = internalNodeNameFromOutputRef(outputRef);
						const payloadType = String((out as any)?.typedSchema?.type ?? '').trim().toLowerCase();
						return {
							id: effectiveName,
							label: internalName || effectiveName,
							payloadType: payloadType.length > 0 ? payloadType : undefined
						};
					})
					.filter((v): v is { id: string; label: string; payloadType?: string } => Boolean(v))
			: null;
</script>

<BaseNode {id} {data} {selected} sourceHandles={outputHandles} showSourceHandleLabels={true}>
	<div style="font-size:12px; opacity:0.9;">
		Component: {componentId}
	</div>
	<div style="font-size:11px; opacity:0.75; margin-top:2px;" title={revisionId}>
		rev {revisionDisplay}
	</div>
	<div style="font-size:11px; opacity:0.75; margin-top:2px;">
		api in {inputCount} / out {outputCount}
	</div>
	<svelte:fragment slot="footer-right">
		{#if checkpointTotal > 0}
			<span class={`checkpointBadge ${checkpointBadgeTone}`} title="Internal component checkpoints">
				{checkpointBadgeText}
			</span>
		{/if}
	</svelte:fragment>
</BaseNode>

<style>
	.checkpointBadge {
		display: inline-flex;
		align-items: center;
		gap: 4px;
		border-radius: 999px;
		padding: 2px 8px;
		font-size: 11px;
		line-height: 1.2;
	}

	.checkpointBadgeValid {
		border: 1px solid rgba(34, 197, 94, 0.45);
		color: #86efac;
		background: rgba(22, 101, 52, 0.25);
	}

	.checkpointBadgeStale {
		border: 1px solid rgba(245, 158, 11, 0.45);
		color: #fcd34d;
		background: rgba(120, 53, 15, 0.28);
	}
</style>
