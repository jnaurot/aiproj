<script lang="ts">
	import { Handle, Position } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import { graphStore, deriveNodeIoForData } from '$lib/flow/store/graphStore';
	import { displayStatusFromBinding } from '$lib/flow/store/runScope';
	import { resolveNodeHandles, type NodeHandleDef } from './portHandles';


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

	// IO contracts are derived from node kind/params.
	$: derivedIo = data ? deriveNodeIoForData(data) : { in: null, out: null };
	$: inputType = derivedIo.in ?? null;
	$: outputType = derivedIo.out ?? null;
	$: effectiveTargetHandles = resolveNodeHandles(data, 'in', targetHandles, inputType);
	$: effectiveSourceHandles = resolveNodeHandles(data, 'out', sourceHandles, outputType);

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
		class={`portHandle portHandle-target plane-${h.plane ?? 'work'}`}
		style={`top:${handleTop(i, effectiveTargetHandles.length)};`}
	/>
{/each}

{#each effectiveSourceHandles as h, i (`source:${h.id}`)}
	<Handle
		type="source"
		position={Position.Right}
		id={h.id}
		class={`portHandle portHandle-source plane-${h.plane ?? 'work'}`}
		style={`top:${handleTop(i, effectiveSourceHandles.length)};`}
	/>
{/each}

<div class={`node ${selected ? 'selected' : ''} st-${status}`}>
	<div class="title">
		<span class="label">{label}</span>
		<span class="badge">{kind}</span>
	</div>

	<slot />

	{#if effectiveTargetHandles.length > 0}
		<div class="targetLabels">
			{#each effectiveTargetHandles as h, i (`label-in:${h.id}`)}
				<div class="targetLabel" style={`top:${handleTop(i, effectiveTargetHandles.length)};`}>
					<span class={`planeBadge plane-${h.plane ?? 'work'}`}>{h.plane ?? 'work'}</span>
					<span class="portText">{h.label ?? h.id}</span>
				</div>
			{/each}
		</div>
	{/if}

	{#if effectiveSourceHandles.length > 0}
		<div class="sourceLabels">
			{#each effectiveSourceHandles as h, i (`label-out:${h.id}`)}
				<div class="sourceLabel" style={`top:${handleTop(i, effectiveSourceHandles.length)};`}>
					<span class="portText">{h.label ?? h.id}</span>
					<span class={`planeBadge plane-${h.plane ?? 'work'}`}>{h.plane ?? 'work'}</span>
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
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}

	.targetLabels {
		position: absolute;
		left: 10px;
		top: 0;
		bottom: 0;
		width: 110px;
		pointer-events: none;
	}

	.targetLabel {
		position: absolute;
		transform: translateY(-50%);
		left: 12px;
		max-width: 100%;
		font-size: 10px;
		line-height: 1;
		opacity: 0.72;
		text-align: left;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}

	.portText {
		max-width: 68px;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.planeBadge {
		border: 1px solid rgba(255, 255, 255, 0.2);
		border-radius: 999px;
		padding: 1px 6px;
		font-size: 9px;
		line-height: 1;
		text-transform: lowercase;
	}

	.planeBadge.plane-work {
		color: #8ab4ff;
		border-color: rgba(138, 180, 255, 0.45);
	}

	.planeBadge.plane-param {
		color: #f2cc60;
		border-color: rgba(242, 204, 96, 0.45);
	}

	.planeBadge.plane-control {
		color: #7ee787;
		border-color: rgba(126, 231, 135, 0.45);
	}

	:global(.portHandle) {
		width: 13px;
		height: 13px;
		border: 2px solid #0f1115;
		box-shadow: 0 0 0 1px rgba(255, 255, 255, 0.16);
	}

	:global(.portHandle.plane-work) {
		background: #4b8cff;
	}

	:global(.portHandle.plane-param) {
		background: #d8ac3f;
	}

	:global(.portHandle.plane-control) {
		background: #2fbf71;
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
