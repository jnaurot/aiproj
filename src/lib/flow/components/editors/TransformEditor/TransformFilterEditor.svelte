<!-- <script lang="ts">
	// lib/flow/components/editors/SourceEditor/TransformFilterEditor.svelte
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';

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
	<div class="field">
		<div class="k">expression</div>
		<div class="v">
			<input
				type="text"
				class="form-control"
				value={params.expr ?? 'length(text) > 10'}
				placeholder="e.g. length(text) > 10"
				on:input={(e) => onDraft({ expr: (e.currentTarget as HTMLInputElement).value })}
			/>
		</div>
	</div>
{/if} -->
<script lang="ts">
	// src/lib/flow/components/editors/TransformEditor/TransformFilterEditor.svelte

	import type { Node } from '@xyflow/svelte';

	// If your FE types differ, adjust these two imports to match your project.
	import type { TransformNodeData } from '$lib/flow/types/transform';

	// Zod-inferred type for the filter op params (expr)
	import type { TransformFilterParams } from '$lib/flow/schema/transform';

	// Optional: if you have a PortsEditor already, reuse it.
	// If you do NOT want ports editable here, you can remove PortsEditor and show read-only ports.
	import PortsEditor from '$lib/flow/components/PortsEditor.svelte';

	// Your app uses the inspector draft as single source of truth.
	// We follow your existing signature: editor receives `params` and onDraft/onCommit.
	//
	// NOTE: Here `params` is the whole node params object for transform,
	// but because you said â€œseparate TransformEditor for each opâ€, weâ€™ll define a local shape
	// that includes enabled/notes/cache + filter-specific object.
	type TransformFilterNodeParams = {
		enabled?: boolean;
		notes?: string;
		cache?: { enabled?: boolean; key?: string };
		// You currently store op inside params on backend normalize_transform_params expects params["op"]
		op: 'filter';
		filter: TransformFilterParams;
	};

	// ---- props (match the way NodeInspector calls editors) ----
	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformFilterNodeParams; // comes from inspector draft
	export let onDraft: (patch: Record<string, any>) => void;
	export let onCommit: (patch: Record<string, any>) => void;

	const DEFAULTS: TransformFilterNodeParams = {
		op: 'filter',
		enabled: true,
		notes: '',
		cache: { enabled: false },
		filter: { expr: 'length(text) > 10' }
	};

	// ---- helpers: keep op editors consistent ----
	function draft(patch: Partial<TransformFilterNodeParams>) {
		onDraft(patch as any);
	}

	function commit(patch: Partial<TransformFilterNodeParams>) {
		onCommit(patch as any);
	}

	// Ensure required shape exists (especially when node first created or older graphs loaded)
	// Do NOT auto-commit on mount unless necessary; draft is enough and user can Accept.
	$: {
		if (!params || params.op !== 'filter') {
			// If someone wires the wrong editor to the wrong node, fail safe.
			// You could also early-return from markup.
		} else {
			// backfill missing blocks
			if (!params.filter) draft({ filter: DEFAULTS.filter });
			if (params.enabled === undefined) draft({ enabled: DEFAULTS.enabled });
			if (params.notes === undefined) draft({ notes: DEFAULTS.notes });
			if (!params.cache) draft({ cache: DEFAULTS.cache });
		}
	}

	// Read current values with safe fallbacks
	$: enabled = params?.enabled ?? DEFAULTS.enabled;
	$: notes = params?.notes ?? DEFAULTS.notes;
	$: expr = params?.filter?.expr ?? DEFAULTS.filter.expr;

	// Ports: you said you want ports shown here too.
	// If PortsEditor already exists and is correct, keep it.
	// Otherwise you can replace with a simple read-only display.
</script>

<div class="transformOp">
	<div class="section">
		<!-- Enabled -->
		<div class="field">
			<div class="k">enabled</div>
			<div class="v">
				<input
					type="checkbox"
					checked={enabled}
					on:change={(e) => draft({ enabled: (e.currentTarget as HTMLInputElement).checked })}
				/>
			</div>
		</div>

		<!-- Notes -->
		<div class="field">
			<div class="k">notes</div>
			<div class="v">
				<textarea
					rows="2"
					value={notes}
					placeholder="Optional notes (UI-only; not hashed if you strip it in normalize_transform_params)"
					on:input={(e) => draft({ notes: (e.currentTarget as HTMLTextAreaElement).value })}
				/>
			</div>
		</div>

		<!-- Cache (optional; keep if you still show it in editors) -->
		<div class="field">
			<div class="k">cache.enabled</div>
			<div class="v">
				<input
					type="checkbox"
					checked={params?.cache?.enabled ?? false}
					on:change={(e) =>
						draft({
							cache: {
								...(params?.cache ?? {}),
								enabled: (e.currentTarget as HTMLInputElement).checked
							}
						})}
				/>
			</div>
		</div>

		<!-- Filter expression -->
		<div class="field">
			<div class="k">filter.expr</div>
			<div class="v">
				<input
					type="text"
					value={expr}
					placeholder="e.g. length(text) > 10"
					on:input={(e) =>
						draft({
							op: 'filter',
							filter: { expr: (e.currentTarget as HTMLInputElement).value }
						})}
					on:blur={() =>
						commit({
							op: 'filter',
							filter: { expr }
						})}
				/>

			</div>
		</div>

		<!-- Quick reset to defaults -->
		<div class="actions">
			<button
				on:click={() => {
					// Replace the whole params to ensure union shape is clean
					commit(DEFAULTS);
				}}
			>
				Reset to defaults
			</button>
		</div>
	</div>
</div>

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

	.subTitle {
		margin-top: 10px;
		font-weight: 600;
		font-size: 13px;
		opacity: 0.95;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-bottom: 10px;
		line-height: 1.35;
	}

	.row {
		display: flex;
		gap: 8px;
		align-items: flex-start;
		margin-bottom: 8px;
	}

	.line {
		display: flex;
		gap: 8px;
		align-items: center;
		margin-bottom: 8px;
	}

	.field {
		display: grid;
		grid-template-columns: 100px minmax(0, 1fr);
		align-items: start;
		gap: 8px;
		margin-bottom: 10px;
	}

	.field.grow {
		flex: 1;
	}

	.field.dir {
		grid-template-columns: 70px minmax(0, 1fr);
	}

	.k,
	.label {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
		font-weight: 400;
	}

	.v {
		min-width: 0;
		width: 100%;
	}

	.colInput {
		flex: 1;
	}

	.arrow {
		opacity: 0.75;
		padding-top: 8px;
	}

	.toggle {
		display: inline-flex;
		gap: 8px;
		align-items: center;
	}

	input,
	select,
	textarea,
	.readonly,
	.code {
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

	textarea,
	.code {
		resize: vertical;
		line-height: 1.35;
		min-height: 96px;
	}

	input[type='checkbox'] {
		width: auto;
		min-height: 0;
		padding: 0;
	}

	input:focus,
	select:focus,
	textarea:focus,
	.code:focus,
	.readonly:focus {
		border-color: rgba(255, 255, 255, 0.25);
	}

	.actions,
	.snips {
		margin-top: 8px;
		display: flex;
		gap: 8px;
		justify-content: flex-end;
		flex-wrap: wrap;
	}

	.snipsTitle {
		font-size: 12px;
		opacity: 0.8;
		align-self: center;
	}

	.snipRow {
		display: flex;
		gap: 8px;
		width: 100%;
	}

	button.small {
		padding: 6px 10px;
		font-size: 12px;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.06);
		color: inherit;
		cursor: pointer;
	}

	button.ghost {
		background: transparent;
	}

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
		color: #fecaca;
	}

	.warn {
		margin-top: 8px;
		font-size: 12px;
		color: #fca5a5;
		white-space: pre-wrap;
	}

	.warn ul {
		margin: 6px 0 0 16px;
		padding: 0;
	}

	.preview {
		margin-top: 12px;
	}

	pre {
		white-space: pre-wrap;
		word-break: break-word;
		padding: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		border-radius: 10px;
		font-size: 12px;
		opacity: 0.95;
	}

	code {
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
		font-size: 12px;
	}
</style>

