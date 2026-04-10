<script lang="ts">
	import { onDestroy } from 'svelte';
	import { Handle, Position, useUpdateNodeInternals } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
import {
	graphStore,
	deriveNodeIoForData,
	getNodeDocExplanationModeFromState,
	getNodeDocPlanesExpansionDelayMsFromState,
	getNodeDocPlanesExpansionEnabledFromState,
	getNodeDocTrainingModeFromState,
	getNodeDocTooltipEnabledFromState,
	getNodeDocTooltipOpenDelayMsFromState,
	getNodeDocResolvedFromState
} from '$lib/flow/store/graphStore';
import NodeDocTooltip from '$lib/flow/components/NodeDocTooltip.svelte';
import { createNodeDocTooltipState, type NodeDocTooltipState } from '$lib/flow/components/nodeDocTooltipState';
import { buildNodeDocLlmContext, buildNodeDocLlmContextSignature } from '$lib/flow/components/nodeDocLlmContext';
import MemoIndicator from './MemoIndicator.svelte';
	import { statusProjectionFromBinding } from '$lib/flow/store/runScope';
	import {
		reconcileLifecycleForActiveRun,
		reconcileModelLeaseLifecycle,
		toDisplayNodeStatus
	} from '$lib/flow/store/statusModel';
	import { portHintText, resolveNodeHandles, type NodeHandleDef } from './portHandles';
	import {
		buildNodeExecutionBadge,
		normalizeConsumeMode,
		resolveNodeRuntimeCounts
	} from './nodeExecutionBadge';


	// xyflow passes these props into node components
	export let id: string;
	export let data: PipelineNodeData;

	// xyflow also passes some optional props (safe to accept)
	export let selected: boolean = false;
	export let sourceHandles: NodeHandleDef[] | null = null;
	export let targetHandles: NodeHandleDef[] | null = null;
	export let showSourceHandleLabels: boolean = false;

	// Status is derived from bindings; node.data.status is not authoritative.
	$: binding = $graphStore.nodeBindings?.[id];
	$: memoState = ((binding as any)?.memoState ?? undefined) as
		| { decision: 'reuse' | 'compute' | 'skip_non_memoizable'; memoKey?: string; resolvedAt?: string }
		| undefined;
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
	$: activeRunLifecycle = reconcileLifecycleForActiveRun({
		lifecycle: statusProjection.lifecycle,
		consumeMode,
		runStatus,
		inflight: inflightCount,
		pendingInputCount,
		readyWork,
		blockedReasonCode
	});
	$: effectiveLifecycle = reconcileModelLeaseLifecycle({
		lifecycle: activeRunLifecycle,
		nodeKind: kind,
		hasActiveLeaseStar: Boolean((data as any)?.meta?.llmAllocated),
		runStatus
	});
	$: status = toDisplayNodeStatus(effectiveLifecycle, statusProjection.freshness);
	$: lifecycleLabel = effectiveLifecycle;
	$: freshnessHint =
		effectiveLifecycle === 'completed' && statusProjection.freshness === 'stale' ? ' (stale)' : '';
	$: kind = data?.kind ?? 'node';
	$: label = data?.label ?? 'Node';
	$: freezeMeta = (data as any)?.meta?.freeze;
	$: freezeMode =
		freezeMeta && freezeMeta.enabled === true && (freezeMeta.mode === 'per_run' || freezeMeta.mode === 'sticky')
			? freezeMeta.mode
			: null;
	$: freezeIcon = freezeMode === 'sticky' ? '#' : '';
	$: freezeClass = freezeMode === 'sticky' ? 'freeze-sticky' : freezeMode === 'per_run' ? 'freeze-per-run' : '';
	$: debugEnabled = Boolean((data as any)?.params?.debug?.enabled ?? false);
	$: processingPolicy = (data as any)?.processingPolicy ?? {};
	$: consumeMode = normalizeConsumeMode(processingPolicy);
	$: batchSize = Math.max(1, Number((processingPolicy as any)?.batch_size ?? 1) || 1);
	$: runtimeCounts = resolveNodeRuntimeCounts(($graphStore as any)?.queueRuntime, id);
	$: executionBadge = buildNodeExecutionBadge(consumeMode, runtimeCounts, batchSize);
	$: nodeDoc = getNodeDocResolvedFromState($graphStore as any, id);
	$: nodeDocExplanationMode = getNodeDocExplanationModeFromState($graphStore as any);
	$: nodeDocTrainingMode = getNodeDocTrainingModeFromState($graphStore as any);
	$: nodeDocTooltipEnabled = getNodeDocTooltipEnabledFromState($graphStore as any);
	$: nodeDocTooltipOpenDelayMs = getNodeDocTooltipOpenDelayMsFromState($graphStore as any);
	$: nodeDocPlanesExpansionEnabled = getNodeDocPlanesExpansionEnabledFromState($graphStore as any);
	$: nodeDocPlanesExpansionDelayMs = getNodeDocPlanesExpansionDelayMsFromState($graphStore as any);
	$: nodeDocExplainModel = String(($graphStore as any)?.nodeDocExplainModel ?? 'glm-4.7-flash:latest').trim() || 'glm-4.7-flash:latest';
	$: nodeDocExplainTemperature = Number(($graphStore as any)?.nodeDocExplainTemperature ?? 0.2);
	$: nodeDocExplainTopP = Number(($graphStore as any)?.nodeDocExplainTopP ?? 1.0);
	$: nodeDocExplainMaxTokens = Number(($graphStore as any)?.nodeDocExplainMaxTokens ?? 512);
	$: nodeDocLlmContext = buildNodeDocLlmContext($graphStore as any, id);
	$: nodeDocLlmSignature = buildNodeDocLlmContextSignature(nodeDocLlmContext);
	let tooltipOpen = false;
	let tooltipExpanded = false;
	let tooltipState: NodeDocTooltipState = createNodeDocTooltipState({
		openDelayMs: nodeDocTooltipOpenDelayMs,
		expandDelayMs: nodeDocPlanesExpansionDelayMs,
		onChange: (next) => {
			tooltipOpen = next.open;
			tooltipExpanded = next.expanded;
		}
	});
	let tooltipOwnerNodeId = String(id ?? '');
	let tooltipDelaySignature = `${String(nodeDocTooltipOpenDelayMs)}::${String(nodeDocPlanesExpansionDelayMs)}`;
	$: {
		const nodeKey = String(id ?? '');
		const nextDelaySignature = `${String(nodeDocTooltipOpenDelayMs)}::${String(nodeDocPlanesExpansionDelayMs)}`;
		if (nodeKey !== tooltipOwnerNodeId || nextDelaySignature !== tooltipDelaySignature) {
			tooltipState.destroy();
			tooltipOwnerNodeId = nodeKey;
			tooltipDelaySignature = nextDelaySignature;
			tooltipState = createNodeDocTooltipState({
				openDelayMs: nodeDocTooltipOpenDelayMs,
				expandDelayMs: nodeDocPlanesExpansionDelayMs,
				onChange: (next) => {
					tooltipOpen = next.open;
					tooltipExpanded = next.expanded;
				}
			});
			tooltipOpen = false;
			tooltipExpanded = false;
		}
	}
	onDestroy(() => {
		tooltipState.destroy();
	});

	// IO contracts are derived from node kind/params.
	$: derivedIo = data ? deriveNodeIoForData(data) : { in: null, out: null };
	$: inputType = derivedIo.in ?? null;
	$: outputType = derivedIo.out ?? null;
	$: connectedTargetHandles = (() => {
		const out: NodeHandleDef[] = [];
		for (const edge of $graphStore?.edges ?? []) {
			if (String((edge as any)?.target ?? '') !== String(id)) continue;
			const handleId = String((edge as any)?.targetHandle ?? 'in').trim() || 'in';
			const mode = String(((edge as any)?.data?.mode ?? 'work')).trim().toLowerCase();
			const plane = mode === 'param' || mode === 'control' ? mode : 'work';
			out.push({ id: handleId, plane: plane as 'work' | 'param' | 'control' });
		}
		return out;
	})();
	$: connectedSourceHandles = (() => {
		const out: NodeHandleDef[] = [];
		for (const edge of $graphStore?.edges ?? []) {
			if (String((edge as any)?.source ?? '') !== String(id)) continue;
			const handleId = String((edge as any)?.sourceHandle ?? 'out').trim() || 'out';
			const mode = String(((edge as any)?.data?.mode ?? 'work')).trim().toLowerCase();
			const plane = mode === 'param' || mode === 'control' ? mode : 'work';
			out.push({ id: handleId, plane: plane as 'work' | 'param' | 'control' });
		}
		return out;
	})();
	$: mergedTargetHandles = [...(Array.isArray(targetHandles) ? targetHandles : []), ...connectedTargetHandles];
	$: mergedSourceHandles = [...(Array.isArray(sourceHandles) ? sourceHandles : []), ...connectedSourceHandles];
	$: effectiveTargetHandles = resolveNodeHandles(data, 'in', mergedTargetHandles, inputType);
	$: effectiveSourceHandles = resolveNodeHandles(data, 'out', mergedSourceHandles, outputType);
	const updateNodeInternals = useUpdateNodeInternals();
	let lastLayoutSignature = '';
	$: {
		const nextLayoutSignature = `${String(id)}::in=${effectiveTargetHandles.map((h) => h.id).join('|')}::out=${effectiveSourceHandles.map((h) => h.id).join('|')}`;
		if (nextLayoutSignature !== lastLayoutSignature) {
			lastLayoutSignature = nextLayoutSignature;
			queueMicrotask(() => {
				updateNodeInternals(String(id));
			});
		}
	}

	function handleTop(index: number, total: number): string {
		if (total <= 1) return '50%';
		const top = ((index + 1) / (total + 1)) * 100;
		return `${Math.max(8, Math.min(92, top))}%`;
	}

	function onNodeEnter(): void {
		if (nodeDoc?.disabled || !nodeDocTooltipEnabled || nodeDocExplanationMode === 'none') return;
		tooltipState.enter();
	}

	function onNodeLeave(): void {
		tooltipState.leave();
	}

	function onNodeKeydown(evt: KeyboardEvent): void {
		if (!nodeDocTooltipEnabled || nodeDocExplanationMode === 'none') return;
		tooltipState.keydown(String(evt?.key ?? ''));
	}

	$: if (nodeDocExplanationMode === 'none' && (tooltipOpen || tooltipExpanded)) {
		tooltipState.leave();
	}

	function persistNodeDocGenerated(explanation: unknown): void {
		graphStore.setNodeDocGeneratedExplanation(String(id ?? ''), explanation as any);
	}

	function clearPersistedNodeDocGenerated(): void {
		graphStore.clearNodeDocGeneratedExplanation(String(id ?? ''));
	}
</script>

{#each effectiveTargetHandles as h, i (`target:${h.id}`)}
	<Handle
		type="target"
		position={Position.Left}
		id={h.id}
		class={`portHandle portHandle-target plane-${h.plane ?? 'work'}`}
		title={portHintText('in', h)}
		data-port-hint={portHintText('in', h)}
		style={`top:${handleTop(i, effectiveTargetHandles.length)};`}
	/>
{/each}

{#each effectiveSourceHandles as h, i (`source:${h.id}`)}
	<Handle
		type="source"
		position={Position.Right}
		id={h.id}
		class={`portHandle portHandle-source plane-${h.plane ?? 'work'}`}
		title={portHintText('out', h)}
		data-port-hint={portHintText('out', h)}
		style={`top:${handleTop(i, effectiveSourceHandles.length)};`}
	/>
	{#if showSourceHandleLabels}
		<div
			class="sourceHandleLabel"
			title={String(h?.label ?? h?.id ?? '')}
			style={`top:${handleTop(i, effectiveSourceHandles.length)};`}
		>
			{String(h?.label ?? h?.id ?? '')}
		</div>
	{/if}
{/each}

<div
	class={`node ${selected ? 'selected' : ''} st-${status}`}
	tabindex="0"
	role="button"
	aria-label={`${label} node documentation`}
	on:mouseenter={onNodeEnter}
	on:mouseleave={onNodeLeave}
	on:keydown={onNodeKeydown}
>
	<div class="title">
		<span class="label">{label}</span>
		<span class={`badge ${freezeClass} ${debugEnabled ? 'debugEnabled' : ''}`}>{kind}{freezeIcon ? ` ${freezeIcon}` : ''}</span>
	</div>

	<slot />

	<div class="footer">
		<div class="footerLeft">
			<span class="status">{lifecycleLabel}{freshnessHint}</span>
			<MemoIndicator {memoState} />
		</div>
		<div class="footerRight">
			<span class="modeBadge mono" title={`mode=${executionBadge.mode}`}>
				{executionBadge.label} {executionBadge.detail}
			</span>
			<slot name="footer-right" />
		</div>
	</div>
		<NodeDocTooltip
			doc={nodeDoc}
			open={tooltipOpen}
			expanded={tooltipExpanded}
			planesExpansionEnabled={nodeDocPlanesExpansionEnabled}
			mode={nodeDocExplanationMode}
			trainingMode={nodeDocTrainingMode}
			llmModel={nodeDocExplainModel}
			llmTemperature={nodeDocExplainTemperature}
			llmTopP={nodeDocExplainTopP}
			llmMaxTokens={nodeDocExplainMaxTokens}
			nodeId={id}
			llmContext={nodeDocLlmContext}
			llmSignature={nodeDocLlmSignature}
		onPersistGenerated={persistNodeDocGenerated}
		onClearPersistedGenerated={clearPersistedNodeDocGenerated}
	/>
</div>

<style>
	.node {
		position: relative;
		width: 220px;
		overflow: visible;
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
		white-space: nowrap;
		flex-shrink: 0;
		margin-left: -2px;
		position: relative;
	}

	.badge.freeze-per-run {
		color: #fff1c2;
		border-color: #f59e0b;
		background: rgba(245, 158, 11, 0.22);
		opacity: 1;
	}

	.badge.debugEnabled::after {
		content: '🐞';
		position: absolute;
		right: 2px;
		top: calc(100% + 2px);
		font-size: 11px;
		line-height: 1;
		opacity: 0.95;
		pointer-events: none;
	}

	.badge.freeze-sticky {
		color: #cfe3ff;
		border-color: #3b82f6;
		background: rgba(59, 130, 246, 0.2);
		opacity: 1;
	}

	.footer {
		margin-top: 8px;
		font-size: 12px;
		opacity: 0.85;
		display: flex;
		align-items: baseline;
		justify-content: space-between;
		gap: 8px;
	}

	.footerLeft {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		min-width: 0;
	}

	.footerRight {
		min-width: 0;
		text-align: right;
		opacity: 0.85;
		display: inline-flex;
		align-items: center;
		gap: 6px;
		flex-wrap: wrap;
		justify-content: flex-end;
	}

	.modeBadge {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		border: 1px solid #283044;
		border-radius: 999px;
		padding: 1px 8px;
		line-height: 1.45;
		font-size: 11px;
		color: #d5def0;
		background: rgba(21, 32, 52, 0.65);
		white-space: nowrap;
	}

	.mono {
		font-variant-numeric: tabular-nums;
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

	.sourceHandleLabel {
		position: absolute;
		right: 14px;
		transform: translateY(-50%);
		font-size: 10px;
		line-height: 1;
		color: #b8c6df;
		opacity: 0.95;
		max-width: 120px;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
		pointer-events: none;
		text-align: right;
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
	.st-busy .status {
		color: #9fb3d9;
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
