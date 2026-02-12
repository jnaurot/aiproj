<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData, PortType } from '$lib/flow/types';
	import { PORT_TYPES } from '$lib/flow/types';
	import { graphStore } from '$lib/flow/store/graphStore';
	// import { selectedNodeId } from '../stores';

	export let selectedNode: Node<PipelineNodeData> | null;

	//TODO figure out what defaults should be
	$: kind = selectedNode?.data?.kind;
	$: currentInType = selectedNode?.data?.ports?.in ?? null;
	$: currentOutType = selectedNode?.data?.ports?.out ?? null;

	function updatePorts(inPort: PortType | null, outPort: PortType | null) {
		if (!selectedNode) return;
		const result = graphStore.updateNodeConfig(selectedNode.id, {
			// params: { ...selectedNode.data.params },
			ports: { in: inPort, out: outPort }
		});

		if (!result.ok) {
			console.error('Port update failed:', result.error);
		} else if (result.removedEdgeIds?.length) {
			console.warn(`Removed ${result.removedEdgeIds.length} incompatible edges`);
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
						<option value="">-- no input --</option>
						{#each PORT_TYPES as portType}
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
						<option value="">-- no output --</option>
						{#each PORT_TYPES as portType}
							<option value={portType}>{portType}</option>
						{/each}
					</select>
				</div>
			</div>
		</div>
	</div>
	<hr style="border:1px solid #ccc; margin:3px" />
{/if}
