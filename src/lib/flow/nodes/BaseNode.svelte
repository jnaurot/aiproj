<script lang="ts">
	import { Handle, Position } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import { graphStore, derivePortsForNodeData } from '$lib/flow/store/graphStore';
	import { displayStatusFromBinding } from '$lib/flow/store/runScope';

	type NodeHandleDef = { id: string; label?: string };

	// xyflow passes these props into node components
	export let id: string;
	export let data: PipelineNodeData;

	// xyflow also passes some optional props (safe to accept)
	export let selected: boolean = false;
	export let sourceHandles: NodeHandleDef[] | null = null;
	export let targetHandles: NodeHandleDef[] | null = null;

	// Status is derived from bindings; node.data.status is not authoritative.
	$: binding = $graphStore.nodeBindings?.[id];
	$: status = displayStatusFromBinding(binding as any);
	$: kind = data?.kind ?? 'node';
	$: label = data?.label ?? 'Node';

	// Port contracts are derived from node kind/params.
	$: derivedPorts = data ? derivePortsForNodeData(data) : { in: null, out: null };
	$: inPort = derivedPorts.in ?? null;
	$: outPort = derivedPorts.out ?? null;
	$: effectiveTargetHandles =
		Array.isArray(targetHandles) && targetHandles.length > 0
			? targetHandles.filter((h) => String(h?.id ?? '').trim().length > 0)
			: inPort !== null
				? [{ id: 'in' }]
				: [];
	$: effectiveSourceHandles =
		Array.isArray(sourceHandles) && sourceHandles.length > 0
			? sourceHandles.filter((h) => String(h?.id ?? '').trim().length > 0)
			: outPort !== null
				? [{ id: 'out' }]
				: [];

	function handleTop(index: number, total: number): string {
		if (total <= 1) return '50%';
		const top = ((index + 1) / (total + 1)) * 100;
		return `${Math.max(8, Math.min(92, top))}%`;
	}
</script>

{#each effectiveTargetHandles as h, i (`target:${h.id}`)}
	<Handle
		type="target"
		position={Position.Left}
		id={h.id}
		style={`top:${handleTop(i, effectiveTargetHandles.length)};`}
	/>
{/each}

{#each effectiveSourceHandles as h, i (`source:${h.id}`)}
	<Handle
		type="source"
		position={Position.Right}
		id={h.id}
		style={`top:${handleTop(i, effectiveSourceHandles.length)};`}
	/>
{/each}

<div class={`node ${selected ? 'selected' : ''} st-${status}`}>
	<div class="title">
		<span class="label">{label}</span>
		<span class="badge">{kind}</span>
	</div>

	<slot />

	{#if effectiveSourceHandles.length > 1}
		<div class="sourceLabels">
			{#each effectiveSourceHandles as h, i (`label:${h.id}`)}
				<div class="sourceLabel" style={`top:${handleTop(i, effectiveSourceHandles.length)};`}>
					{h.label ?? h.id}
				</div>
			{/each}
		</div>
	{/if}

	<div class="footer">
		<span class="status">{status}</span>
	</div>
</div>

<style>
	.node {
		position: relative;
		width: 220px;
		border-radius: 12px;
		border: 1px solid #2a2a2a;
		background: #0f1115;
		color: #e6e6e6;
		padding: 10px;
		box-shadow: 0 8px 18px rgba(0, 0, 0, 0.35);
	}

	.node.selected {
		outline: 2px solid #4b8cff;
	}

	.title {
		display: flex;
		align-items: center;
		justify-content: space-between;
		font-weight: 600;
		margin-bottom: 8px;
		gap: 10px;
	}

	.label {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.badge {
		font-size: 12px;
		opacity: 0.8;
		border: 1px solid #283044;
		border-radius: 999px;
		padding: 2px 8px;
	}

	.footer {
		margin-top: 8px;
		font-size: 12px;
		opacity: 0.85;
	}

	.sourceLabels {
		position: absolute;
		right: 10px;
		top: 0;
		bottom: 0;
		width: 92px;
		pointer-events: none;
	}

	.sourceLabel {
		position: absolute;
		transform: translateY(-50%);
		right: 12px;
		max-width: 100%;
		font-size: 10px;
		line-height: 1;
		opacity: 0.72;
		text-align: right;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	/* status coloring */
	.st-idle .status {
		color: #e6e6e6;
	}
	.st-stale .status {
		color: #f2cc60;
	}
	.st-running .status {
		color: #8ab4ff;
	}
	.st-succeeded .status {
		color: #7ee787;
	}
	.st-failed .status {
		color: #ff7b72;
	}
	.st-canceled .status {
		color: #f2cc60;
	}
</style>
