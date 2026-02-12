<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import NumberInput from '$lib/flow/components/NumberInput.svelte';
	import KeyValueEditor from '$lib/flow/components/KeyValueEditor.svelte';
	import { graphStore } from '$lib/flow/store/graphStore';

	export let selectedNode: Node<PipelineNodeData> | null;

	function patchParams(patch: Record<string, any>) {
		if (!selectedNode) return;
		const cur = { ...selectedNode.data.params, ...patch };
		const newPorts = { ...selectedNode.data.ports };
		graphStore.updateNodeConfig(selectedNode.id, { params: cur, ports: newPorts });
	}

	$: params = (selectedNode?.data?.params ?? {}) as any;
</script>

{#if selectedNode}
	<div class="section">
		<div class="group">
			<div class="field">
				<div class="k">url</div>
				<div class="v">
					<input
						value={params.url ?? ''}
						placeholder="https://api.example.com/data"
						on:input={(e) => patchParams({ url: (e.currentTarget as HTMLInputElement).value })}
					/>
				</div>
			</div>

			<div class="field">
				<div class="k">method</div>
				<div class="v">
					<select
						value={params.method ?? 'GET'}
						on:change={(e) => patchParams({ method: (e.currentTarget as HTMLSelectElement).value })}
					>
						<option value="GET">GET</option>
						<option value="POST">POST</option>
						<option value="PUT">PUT</option>
						<option value="DELETE">DELETE</option>
					</select>
				</div>
			</div>

			<div class="field">
				<div class="k">auth_type</div>
				<div class="v">
					<select
						value={params.auth_type ?? 'none'}
						on:change={(e) =>
							patchParams({ auth_type: (e.currentTarget as HTMLSelectElement).value })}
					>
						<option value="none">none</option>
						<option value="bearer">bearer</option>
						<option value="basic">basic</option>
						<option value="api_key">api_key</option>
					</select>
				</div>
			</div>

			<div class="field">
				<div class="k">auth_token_ref</div>
				<div class="v">
					<input
						value={params.auth_token_ref ?? ''}
						placeholder="ENV_VAR_NAME (required if auth_type != none)"
						on:input={(e) => {
							const v = (e.currentTarget as HTMLInputElement).value.trim();
							patchParams({ auth_token_ref: v === '' ? undefined : v });
						}}
					/>
				</div>
			</div>
			<div class="field">
				<div class="k">timeout_seconds</div>
				<div class="v">
					<NumberInput
						value={params.timeout_seconds ?? 30}
						min={1}
						onChange={(v) => patchParams({ timeout_seconds: v ?? 30 })}
					/>
				</div>
			</div>
		</div>
		<div class="field">
			<div class="k">headers</div>
			<div class="v">
				<KeyValueEditor
					label=""
					value={params.headers ?? {}}
					allowTypes={false}
					onChange={(next) => patchParams({ headers: next as any })}
				/>
			</div>
		</div>

		<div class="field">
			<div class="k">body</div>
			<div class="v">
				<KeyValueEditor
					label=""
					value={params.body ?? {}}
					allowTypes={true}
					defaultType="string"
					onChange={(next) => {
						const keys = next ? Object.keys(next) : [];
						patchParams({ body: keys.length ? (next as any) : undefined });
					}}
				/>
			</div>
		</div>
	</div>
{/if}
