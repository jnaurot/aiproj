<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData, PortType } from '$lib/flow/types';
	import { graphStore } from '$lib/flow/store/graphStore';
	import { getAllowedPortsForNode } from '$lib/flow/portCapabilities';
	// import { selectedNodeId } from '../stores';

	export let selectedNode: Node<PipelineNodeData> | null;

	//TODO figure out what defaults should be
	$: kind = selectedNode?.data?.kind;
	$: currentInType = selectedNode?.data?.ports?.in ?? null;
	$: currentOutType = selectedNode?.data?.ports?.out ?? null;
	function withCurrent(base: PortType[], current: PortType | null): PortType[] {
		if (current && !base.includes(current)) return [current, ...base];
		return base;
	}
	$: allowedInPorts = (getAllowedPortsForNode(selectedNode as any, 'in') ?? []) as PortType[];
	$: allowedOutPorts = (getAllowedPortsForNode(selectedNode as any, 'out') ?? []) as PortType[];
	$: inPortOptions = withCurrent(allowedInPorts as PortType[], currentInType);
	$: outPortOptions = withCurrent(allowedOutPorts as PortType[], currentOutType);
	let configError: string | null = null;

	function updatePorts(inPort: PortType | null, outPort: PortType | null) {
		if (!selectedNode) return;
		const result = graphStore.updateNodeConfig(selectedNode.id, {
			// params: { ...selectedNode.data.params },
			ports: { in: inPort, out: outPort }
		});

		if (!result.ok) {
			configError = result.error ?? 'Port update failed';
		} else if (result.removedEdgeIds?.length) {
			configError = null;
		} else {
			configError = null;
		}
	}
</script>

{#if selectedNode}
	<div class="portsTheme">
	<div class="section">
		<div class="sectionTitle">Ports</div>

		<div class="group">
			<div class="field field-inline">
				<div class="k">inPort</div>
				<div class="v">
					<select
						value={currentInType ?? ''}
						on:change={(e) => {
							const val = e.currentTarget.value;
							const inPort = val === '' ? null : (val as PortType);
							updatePorts(inPort, currentOutType);
						}}
					>
						{#if kind === 'source'}
							<option value="">-- no input --</option>
						{/if}
						{#each inPortOptions as portType}
							<option value={portType}>{portType}</option>
						{/each}
					</select>
				</div>
			</div>

			<div class="field field-inline">
				<div class="k">outPort</div>
				<div class="v">
					<select
						value={currentOutType}
						on:change={(e) => {
							const val = e.currentTarget.value;
							const outPort = val === '' ? null : (val as PortType);
							updatePorts(currentInType, outPort);
						}}
					>
						{#each outPortOptions as portType}
							<option value={portType}>{portType}</option>
						{/each}
					</select>
				</div>
			</div>
		</div>
	</div>
	{#if configError}
		<div class="configError">{configError}</div>
	{/if}
	<hr class="divider" />
	</div>
{/if}

<style>
	.portsTheme {
		--pe-card: #ffffff;
		--pe-border: #d7deea;
		--pe-text: #1f2937;
		--pe-muted: #5b6677;
		--pe-control-bg: #ffffff;
		--pe-control-text: #1f2937;
		--pe-control-border: #b9c5da;
		--pe-divider: #d7deea;
		--pe-error-bg: #fee2e2;
		--pe-error-border: #fca5a5;
		--pe-error-text: #7f1d1d;
		color: var(--pe-text);
	}

	@media (prefers-color-scheme: dark) {
		.portsTheme {
			--pe-card: #0f1724;
			--pe-border: #253049;
			--pe-text: #e5e7eb;
			--pe-muted: #9aa3b2;
			--pe-control-bg: #0b1220;
			--pe-control-text: #e5e7eb;
			--pe-control-border: #2c3b59;
			--pe-divider: #253049;
			--pe-error-bg: rgba(239, 68, 68, 0.12);
			--pe-error-border: rgba(239, 68, 68, 0.45);
			--pe-error-text: #fecaca;
		}
	}

	:global(.portsTheme .section) {
		background: var(--pe-card);
		border: 1px solid var(--pe-border);
		border-radius: 10px;
		padding: 8px 10px;
	}

	:global(.portsTheme .sectionTitle) {
		color: var(--pe-text);
	}

	:global(.portsTheme .k) {
		color: var(--pe-muted);
	}

	:global(.portsTheme .v input),
	:global(.portsTheme .v select),
	:global(.portsTheme .v textarea) {
		background: var(--pe-control-bg);
		color: var(--pe-control-text);
		border: 1px solid var(--pe-control-border);
	}

	:global(.portsTheme .v select option) {
		background: var(--pe-control-bg);
		color: var(--pe-control-text);
	}

	.divider {
		border: 1px solid var(--pe-divider);
		margin: 6px 0 3px;
	}

	.configError {
		margin-top: 8px;
		padding: 8px 10px;
		border-radius: 8px;
		border: 1px solid var(--pe-error-border);
		background: var(--pe-error-bg);
		color: var(--pe-error-text);
		font-size: 12px;
	}
</style>
