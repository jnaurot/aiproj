<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import { ToolEditorByProvider } from './ToolEditor';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Record<string, any>;
	export let onDraft: (patch: Record<string, any>) => void;
	export let onCommit: (patch: Record<string, any>) => void;

	$: provider = (params?.provider ?? 'mcp') as keyof typeof ToolEditorByProvider;
	$: name = typeof params?.name === 'string' ? params.name : '';
	$: toolVersion = typeof params?.toolVersion === 'string' ? params.toolVersion : 'v1';
	$: sideEffectMode = (params?.side_effect_mode ?? 'pure') as 'pure' | 'idempotent' | 'effectful';
	$: armed = Boolean(params?.armed ?? false);
	$: connectionRef = typeof params?.connectionRef === 'string' ? params.connectionRef : '';
	$: timeoutMs = params?.timeoutMs;
	$: retry = params?.retry ?? {};
	$: maxAttempts = Number(retry?.max_attempts ?? 1);
	$: backoffMs = Number(retry?.backoff_ms ?? 0);
	$: permissions = params?.permissions ?? {};
	$: canNet = Boolean(permissions?.net ?? false);
	$: canFs = Boolean(permissions?.fs ?? false);
	$: canEnv = Boolean(permissions?.env ?? false);
	$: canSubprocess = Boolean(permissions?.subprocess ?? false);

	function patch(p: Record<string, any>) {
		onDraft(p);
	}

	function commit(p: Record<string, any>) {
		onCommit(p);
	}
</script>

<div class="section">
	<div class="sectionTitle">Tool Config</div>

	<div class="field">
		<div class="k">name</div>
		<div class="v">
			<input
				type="text"
				value={name}
				placeholder="http.request"
				on:input={(e) => patch({ name: (e.currentTarget as HTMLInputElement).value })}
				on:blur={(e) => commit({ name: (e.currentTarget as HTMLInputElement).value })}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">toolVersion</div>
		<div class="v">
			<input
				type="text"
				value={toolVersion}
				placeholder="v1"
				on:input={(e) => patch({ toolVersion: (e.currentTarget as HTMLInputElement).value })}
				on:blur={(e) => commit({ toolVersion: (e.currentTarget as HTMLInputElement).value })}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">side_effect_mode</div>
		<div class="v">
			<select
				value={sideEffectMode}
				on:change={(e) => {
					const v = (e.currentTarget as HTMLSelectElement).value;
					patch({ side_effect_mode: v });
					commit({ side_effect_mode: v });
				}}
			>
				<option value="pure">pure</option>
				<option value="idempotent">idempotent</option>
				<option value="effectful">effectful</option>
			</select>
		</div>
	</div>

	<div class="field">
		<div class="k">armed</div>
		<div class="v">
			<input
				type="checkbox"
				checked={armed}
				on:change={(e) => {
					const v = (e.currentTarget as HTMLInputElement).checked;
					patch({ armed: v });
					commit({ armed: v });
				}}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">connectionRef</div>
		<div class="v">
			<input
				type="text"
				value={connectionRef}
				placeholder="conn:default"
				on:input={(e) => patch({ connectionRef: (e.currentTarget as HTMLInputElement).value })}
				on:blur={(e) => commit({ connectionRef: (e.currentTarget as HTMLInputElement).value })}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">timeoutMs</div>
		<div class="v">
			<input
				type="number"
				min="1"
				value={timeoutMs ?? ''}
				on:input={(e) =>
					patch({ timeoutMs: Number((e.currentTarget as HTMLInputElement).value) || undefined })}
				on:blur={(e) =>
					commit({ timeoutMs: Number((e.currentTarget as HTMLInputElement).value) || undefined })}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">retry.max_attempts</div>
		<div class="v">
			<input
				type="number"
				min="1"
				value={maxAttempts}
				on:input={(e) =>
					patch({
						retry: {
							...(retry ?? {}),
							max_attempts: Number((e.currentTarget as HTMLInputElement).value) || 1
						}
					})}
				on:blur={(e) =>
					commit({
						retry: {
							...(retry ?? {}),
							max_attempts: Number((e.currentTarget as HTMLInputElement).value) || 1
						}
					})}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">retry.backoff_ms</div>
		<div class="v">
			<input
				type="number"
				min="0"
				value={backoffMs}
				on:input={(e) =>
					patch({
						retry: {
							...(retry ?? {}),
							backoff_ms: Number((e.currentTarget as HTMLInputElement).value) || 0
						}
					})}
				on:blur={(e) =>
					commit({
						retry: {
							...(retry ?? {}),
							backoff_ms: Number((e.currentTarget as HTMLInputElement).value) || 0
						}
					})}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">perm.net</div>
		<div class="v">
			<input
				type="checkbox"
				checked={canNet}
				on:change={(e) => {
					const v = (e.currentTarget as HTMLInputElement).checked;
					const next = { ...(permissions ?? {}), net: v };
					patch({ permissions: next });
					commit({ permissions: next });
				}}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">perm.fs</div>
		<div class="v">
			<input
				type="checkbox"
				checked={canFs}
				on:change={(e) => {
					const v = (e.currentTarget as HTMLInputElement).checked;
					const next = { ...(permissions ?? {}), fs: v };
					patch({ permissions: next });
					commit({ permissions: next });
				}}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">perm.env</div>
		<div class="v">
			<input
				type="checkbox"
				checked={canEnv}
				on:change={(e) => {
					const v = (e.currentTarget as HTMLInputElement).checked;
					const next = { ...(permissions ?? {}), env: v };
					patch({ permissions: next });
					commit({ permissions: next });
				}}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">perm.subprocess</div>
		<div class="v">
			<input
				type="checkbox"
				checked={canSubprocess}
				on:change={(e) => {
					const v = (e.currentTarget as HTMLInputElement).checked;
					const next = { ...(permissions ?? {}), subprocess: v };
					patch({ permissions: next });
					commit({ permissions: next });
				}}
			/>
		</div>
	</div>
</div>

{#if ToolEditorByProvider[provider]}
	<svelte:component
		this={ToolEditorByProvider[provider]}
		{selectedNode}
		{params}
		{onDraft}
		{onCommit}
	/>
{/if}

<style>
	.section {
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 12px;
		padding: 12px;
		background: rgba(255, 255, 255, 0.03);
		margin-top: 8px;
	}

	.sectionTitle {
		font-weight: 650;
		font-size: 14px;
		margin-bottom: 10px;
		opacity: 0.9;
	}

	.field {
		display: grid;
		grid-template-columns: 100px minmax(0, 1fr);
		align-items: start;
		gap: 8px;
		margin-bottom: 10px;
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

	input[type='checkbox'] {
		width: auto;
		min-height: 0;
		padding: 0;
	}
</style>
