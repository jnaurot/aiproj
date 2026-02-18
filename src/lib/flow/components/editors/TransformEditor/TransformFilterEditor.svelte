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
	// but because you said “separate TransformEditor for each op”, we’ll define a local shape
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
	.transformOp {
		padding: 8px 0;
	}

	.section {
		border: 1px solid #ddd;
		border-radius: 6px;
		padding: 10px;
	}

	.sectionTitle {
		font-weight: 800;
		margin-bottom: 10px;
	}

	.field {
		display: grid;
		grid-template-columns: 120px 1fr;
		gap: 10px;
		align-items: start;
		margin: 10px 0;
	}

	.k {
		font-size: 12px;
		opacity: 0.85;
		padding-top: 6px;
	}

	.v {
		min-width: 0;
	}

	input[type='text'],
	textarea {
		width: 100%;
		box-sizing: border-box;
		padding: 6px 8px;
		border: 1px solid #ccc;
		border-radius: 4px;
	}

	.hint {
		margin-top: 6px;
		font-size: 12px;
		opacity: 0.75;
		line-height: 1.35;
	}

	code {
		font-family:
			ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New',
			monospace;
		font-size: 12px;
	}

	.actions {
		margin-top: 12px;
		display: flex;
		justify-content: flex-end;
	}

	button {
		border: 1px solid #444;
		border-radius: 6px;
		padding: 6px 10px;
		cursor: pointer;
	}
</style>
