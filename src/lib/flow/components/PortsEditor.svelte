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
	<hr style="border:1px solid #ccc; margin:3px" />
{/if}

<style>
	.configError {
		margin-top: 8px;
		padding: 8px 10px;
		border-radius: 8px;
		border: 1px solid rgba(239, 68, 68, 0.45);
		background: rgba(239, 68, 68, 0.12);
		color: #fecaca;
		font-size: 12px;
	}
</style>
