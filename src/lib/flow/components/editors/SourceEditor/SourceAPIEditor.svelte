<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import NumberInput from '$lib/flow/components/NumberInput.svelte';
	import KeyValueEditor from '$lib/flow/components/KeyValueEditor.svelte';

	export let selectedNode: Node<PipelineNodeData> | null;
	export let params: Record<string, any>;
	export let onDraft: (patch: Record<string, any>) => void;
	export let onCommit: (patch: Record<string, any>) => void;

	function draft(patch: Record<string, any>) {
		onDraft?.(patch);
	}

	function commit(patch: Record<string, any>) {
		onCommit?.(patch);
	}
</script>

{#if selectedNode}
	<div class="section">
		<div class="sectionTitle">API</div>
		<div class="group">
			<div class="field">
				<div class="k">url</div>
				<div class="v">
					<input
						value={params.url ?? ''}
						placeholder="https://api.example.com/data"
						on:input={(e) => draft({ url: (e.currentTarget as HTMLInputElement).value })}
						on:blur={(e) => commit({ url: (e.currentTarget as HTMLInputElement).value })}
					/>
				</div>
			</div>

			<div class="field">
				<div class="k">method</div>
				<div class="v">
					<select
						value={params.method ?? 'GET'}
						on:change={(e) => {
							const v = (e.currentTarget as HTMLSelectElement).value;
							draft({ method: v });
							commit({ method: v });
						}}
					>
						<option value="GET">GET</option>
						<option value="POST">POST</option>
						<option value="PUT">PUT</option>
						<option value="PATCH">PATCH</option>
						<option value="DELETE">DELETE</option>
					</select>
				</div>
			</div>

			<div class="field">
				<div class="k">auth_type</div>
				<div class="v">
					<select
						value={params.auth_type ?? 'none'}
						on:change={(e) => {
							const v = (e.currentTarget as HTMLSelectElement).value;
							draft({ auth_type: v });
							commit({ auth_type: v });
						}}
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
							draft({ auth_token_ref: v === '' ? undefined : v });
						}}
						on:blur={(e) => {
							const v = (e.currentTarget as HTMLInputElement).value.trim();
							commit({ auth_token_ref: v === '' ? undefined : v });
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
						onChange={(v) => draft({ timeout_seconds: v ?? 30 })}
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
					onChange={(next) => draft({ headers: next as any })}
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
						draft({ body: keys.length ? (next as any) : undefined });
					}}
				/>
			</div>
		</div>
	</div>
{/if}

<style>
	.section {
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 12px;
		padding: 12px;
		background: rgba(255, 255, 255, 0.03);
	}

	.sectionTitle {
		font-weight: 650;
		font-size: 14px;
		margin-bottom: 10px;
		opacity: 0.9;
	}

	.group {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}

	.field {
		display: grid;
		grid-template-columns: 100px minmax(0, 1fr);
		align-items: start;
		gap: 8px;
	}

	.k {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
	}

	.v {
		min-width: 0;
	}

	input,
	select {
		width: 100%;
		box-sizing: border-box;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(0, 0, 0, 0.2);
		color: inherit;
		padding: 8px 10px;
		font-size: 14px;
		outline: none;
		min-height: 40px;
	}

	input:focus,
	select:focus {
		border-color: rgba(255, 255, 255, 0.25);
	}
</style>
