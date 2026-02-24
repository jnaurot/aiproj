<script lang="ts">
	// lib/flow/components/editors/SourceEditor/SourceDatabaseEditor.svelte
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import NumberInput from '$lib/flow/components/NumberInput.svelte';

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;

	export let params: any;
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
		<div class="sectionTitle">Database</div>
		<div class="group">
			<div class="field">
				<div class="k">connection_string</div>
				<div class="v">
					<input
						value={params.connection_string ?? ''}
						placeholder="postgresql://user:pass@host:5432/db"
						on:input={(e) =>
							draft({ connection_string: (e.currentTarget as HTMLInputElement).value })}
					/>
				</div>
			</div>

			<div class="field">
				<div class="k">connection_ref</div>
				<div class="v">
					<input
						value={params.connection_ref ?? ''}
						placeholder="(optional) secret/env ref"
						on:input={(e) => {
							const v = (e.currentTarget as HTMLInputElement).value.trim();
							draft({ connection_ref: v === '' ? undefined : v });
						}}
					/>
				</div>
			</div>

			<div class="field">
				<div class="k">query</div>
				<div class="v">
					<textarea
						rows="4"
						placeholder="SELECT * FROM table"
						on:input={(e) => {
							const v = (e.currentTarget as HTMLTextAreaElement).value;
							// If user supplies query, table_name becomes optional noise
							draft({ query: v, table_name: v.trim() ? undefined : params.table_name });
						}}>{params.query ?? ''}</textarea
					>
				</div>
			</div>

			<div class="field">
				<div class="k">table_name</div>
				<div class="v">
					<input
						value={params.table_name ?? ''}
						placeholder="(optional) table"
						on:input={(e) => {
							const v = (e.currentTarget as HTMLInputElement).value.trim();
							// If user supplies table_name, query becomes optional noise
							draft({
								table_name: v === '' ? undefined : v,
								query: v ? undefined : params.query
							});
						}}
					/>
				</div>
			</div>
			<div class="field">
				<div class="k">limit</div>
				<div class="v">
					<NumberInput
						value={params.limit}
						placeholder="e.g. 5000"
						min={1}
						onChange={(v) => draft({ limit: v })}
					/>
				</div>
			</div>

			<p class="hint" style="margin-top:8px;">
				Backend requires: (connection_string OR connection_ref) AND (query OR table_name).
			</p>
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
	textarea {
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

	textarea {
		resize: vertical;
		line-height: 1.35;
		min-height: 96px;
	}

	input:focus,
	textarea:focus {
		border-color: rgba(255, 255, 255, 0.25);
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
	}
</style>
