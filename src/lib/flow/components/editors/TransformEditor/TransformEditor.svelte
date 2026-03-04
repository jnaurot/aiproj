<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformParams, TransformKind } from '$lib/flow/schema/transform';
	import { defaultTransformParamsByKind } from '$lib/flow/schema/transformDefaults';
	import { graphStore } from '$lib/flow/store/graphStore';
	import { getArtifactMetaUrl } from '$lib/flow/client/runs';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { TransformEditorByKind } from './TransformEditor';
	import { parseInputSchemaView, type InputSchemaView } from './inputSchema';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformParams>;
	export let onDraft: (patch: Partial<TransformParams>) => void;
	export let onCommit: (patch: Partial<TransformParams>) => void;
	export let nodeError: NodeExecutionError | null = null;

	const ops: TransformKind[] = [
		'filter',
		'select',
		'rename',
		'derive',
		'aggregate',
		'join',
		'sort',
		'limit',
		'dedupe',
		'split',
		'sql'
	];
	function isTransformKind(value: unknown): value is TransformKind {
		return typeof value === 'string' && ops.includes(value as TransformKind);
	}

	function toFiniteInt(value: unknown): number | undefined {
		if (typeof value === 'number' && Number.isFinite(value)) return Math.trunc(value);
		if (typeof value === 'string') {
			const parsed = Number.parseInt(value, 10);
			if (Number.isFinite(parsed)) return parsed;
		}
		return undefined;
	}

	$: void selectedNode?.id;
	$: currentOp = isTransformKind(params?.op) ? params.op : 'filter';
	$: enabled = params?.enabled ?? true;
	$: notes = params?.notes ?? '';
	$: EditorComponent = TransformEditorByKind[currentOp];
	$: limitNested = (params as Record<string, unknown>)?.limit as Record<string, unknown> | undefined;
	$: limitN =
		toFiniteInt(limitNested?.n) ??
		toFiniteInt((limitNested as any)?.limit?.n) ??
		toFiniteInt((params as Record<string, unknown>)?.n) ??
		defaultTransformParamsByKind.limit.limit.n;
	$: childParams =
		currentOp === 'limit'
			? ({ n: limitN } as Record<string, unknown>)
			: ((params as Record<string, unknown>)[currentOp] ??
				defaultTransformParamsByKind[currentOp][currentOp]);
	$: graphState = $graphStore;
	$: splitInputColumns = Array.from(
		new Set(inputSchemas.flatMap((schema) => schema.columns.map((c) => String(c.name || ''))).filter(Boolean))
	);
	$: hasWrappedInput = inputSchemas.some((s) =>
		['json_object_1row', 'text_1row', 'binary_hex_1row'].includes(s.coercion?.mode ?? '')
	);

	let inputSchemas: InputSchemaView[] = [];
	let inputSchemaError: string | null = null;
	let loadingInputSchemas = false;
	let inputSchemaReqSeq = 0;
	let lastInputSignature = '';

	$: if (selectedNode?.id) {
		const nodeId = selectedNode.id;
		const edges = graphState?.edges ?? [];
		const nodeBindings = graphState?.nodeBindings ?? {};
		const incoming = edges
			.filter((e) => e.target === nodeId)
			.map((e) => {
				const sourceBinding = nodeBindings[e.source];
				const artifactId = String(sourceBinding?.currentArtifactId ?? sourceBinding?.lastArtifactId ?? '');
				return `${e.source}:${String(e.targetHandle ?? 'in')}:${artifactId}`;
			})
			.sort();
		const signature = `${String(graphState?.graphId ?? '')}|${nodeId}|${incoming.join('|')}`;
		if (signature !== lastInputSignature) {
			lastInputSignature = signature;
			void refreshInputSchemas();
		}
	} else {
		lastInputSignature = '';
	}

	function handleOpChange(nextOp: TransformKind): void {
		const next = structuredClone(defaultTransformParamsByKind[nextOp]);
		onDraft(next);
		onCommit(next);
	}

	function patchChild(next: Record<string, unknown>): void {
		onDraft({ [currentOp]: next } as Partial<TransformParams>);
	}

	function commitChild(next: Record<string, unknown>): void {
		onCommit({ [currentOp]: next } as Partial<TransformParams>);
	}

	function patchChildFor(opKey: TransformKind, next: Record<string, unknown>) {
		onDraft({ [opKey]: next } as Partial<TransformParams>);
	}

	function commitChildFor(opKey: TransformKind, next: Record<string, unknown>) {
		onCommit({ [opKey]: next } as Partial<TransformParams>);
	}

	async function refreshInputSchemas(): Promise<void> {
		const nodeId = selectedNode?.id;
		if (!nodeId) {
			inputSchemas = [];
			inputSchemaError = null;
			return;
		}
		const reqId = ++inputSchemaReqSeq;
		loadingInputSchemas = true;
		inputSchemaError = null;
		try {
			const edges = graphState?.edges ?? [];
			const nodeBindings = graphState?.nodeBindings ?? {};
			const nodesById = new Map((graphState?.nodes ?? []).map((n) => [n.id, n]));
			const graphId = String(graphState?.graphId ?? '').trim();
			if (!graphId) {
				inputSchemas = [];
				return;
			}
			const incoming = edges
				.filter((e) => e.target === nodeId)
				.map((e) => {
					const sourceBinding = nodeBindings[e.source];
					const artifactId = String(sourceBinding?.currentArtifactId ?? sourceBinding?.lastArtifactId ?? '');
					return {
						sourceNodeId: e.source,
						label: `${String(nodesById.get(e.source)?.data?.label ?? e.source)}.${String(e.targetHandle ?? 'in')}`,
						artifactId
					};
				})
				.filter((x) => x.artifactId.length > 0);
			if (incoming.length === 0) {
				inputSchemas = [];
				return;
			}
			const responses = await Promise.all(
				incoming.map(async (entry) => {
					const res = await fetch(getArtifactMetaUrl(entry.artifactId, graphId));
					if (!res.ok) throw new Error(`Failed to load schema for ${entry.artifactId}: ${res.status}`);
					const meta = await res.json();
					return parseInputSchemaView(
						entry.artifactId,
						entry.label,
						(meta?.schema ?? meta?.payloadSchema) as Record<string, unknown> | undefined
					);
				})
			);
			if (reqId !== inputSchemaReqSeq) return;
			inputSchemas = responses;
		} catch (err) {
			if (reqId !== inputSchemaReqSeq) return;
			inputSchemaError = err instanceof Error ? err.message : String(err);
			inputSchemas = [];
		} finally {
			if (reqId === inputSchemaReqSeq) loadingInputSchemas = false;
		}
	}
</script>

<Section title="Transform">
	<Field label="op">
		<select
			value={currentOp}
			on:change={(event) => handleOpChange((event.currentTarget as HTMLSelectElement).value as TransformKind)}
		>
			{#each ops as op}
				<option value={op}>{op}</option>
			{/each}
		</select>
	</Field>

	<Field label="enabled">
		<Input
			type="checkbox"
			checked={enabled}
			onChange={(event) => onDraft({ enabled: (event.currentTarget as HTMLInputElement).checked })}
		/>
	</Field>

	<Field label="notes">
		<Input
			multiline={true}
			rows={3}
			value={notes}
			onInput={(event) => onDraft({ notes: (event.currentTarget as HTMLTextAreaElement).value })}
		/>
	</Field>
</Section>

<Section title="Input Schema">
	{#if loadingInputSchemas}
		<div class="schemaMuted">Loading input schema...</div>
	{:else if inputSchemaError}
		<div class="schemaError">{inputSchemaError}</div>
	{:else if inputSchemas.length === 0}
		<div class="schemaMuted">Schema unavailable.</div>
	{:else}
		{#each inputSchemas as schema}
			<div class="schemaCard">
				<div class="schemaHeader">
					<div class="schemaLabel">{schema.label}</div>
					<div class="schemaRows">
						rows:
						{schema.rowCount ?? 'unknown'}
					</div>
				</div>
				{#if schema.provenance}
					<div class="schemaProv">
						{#if schema.provenance.tableName}
							<span>table: {schema.provenance.tableName}</span>
						{/if}
						{#if schema.provenance.dbName}
							<span>db: {schema.provenance.dbName}</span>
						{/if}
						{#if schema.provenance.dbSchema}
							<span>schema: {schema.provenance.dbSchema}</span>
						{/if}
						{#if schema.provenance.endpoint}
							<span>endpoint: {schema.provenance.endpoint}</span>
						{/if}
					</div>
				{/if}
				{#if schema.coercion && schema.coercion.mode !== 'native'}
					<div class="schemaCoercion">
						<span>Coerced table: {schema.coercion.mode}</span>
						{#if schema.rowCount !== null}
							<span>rows: {schema.rowCount}</span>
						{/if}
						<span>cols: {schema.columns.length}</span>
						{#if schema.coercion.lossy}
							<span>lossy coercion</span>
						{/if}
						{#if schema.coercion.notes}
							<span>{schema.coercion.notes}</span>
						{/if}
					</div>
				{/if}
				{#if schema.columns.length === 0}
					<div class="schemaMuted">No columns available.</div>
				{:else}
					<div class="schemaCols">
						{#each schema.columns as col}
							<div class="schemaCol">
								<span class="schemaColName">{col.name}</span>
								<span class="schemaColType">{col.type || 'unknown'}</span>
							</div>
						{/each}
					</div>
				{/if}
			</div>
		{/each}
	{/if}
</Section>

{#if currentOp === 'join' && hasWrappedInput}
	<div class="joinHint">
		This input is a wrapped payload; joins/aggregations may not behave as expected.
	</div>
{/if}

{#if EditorComponent}
	{#if currentOp === 'split'}
		<svelte:component
			this={EditorComponent}
			{selectedNode}
			params={childParams}
			onDraft={patchChild}
			onCommit={commitChild}
			inputColumns={splitInputColumns}
		/>
	{:else if currentOp === 'sort'}
	{@const opKey = currentOp}
		<svelte:component
			this={EditorComponent}
			{selectedNode}
			params={childParams}
			onDraft={(next) => patchChildFor(opKey, next)}
			onCommit={(next) => commitChildFor(opKey, next)}
			inputColumns={splitInputColumns}
			{nodeError}
		/>
	{:else if currentOp === 'dedupe'}
	{@const opKey = currentOp}
		<svelte:component
			this={EditorComponent}
			{selectedNode}
			params={childParams}
			onDraft={(next) => patchChildFor(opKey, next)}
			onCommit={(next) => commitChildFor(opKey, next)}
			inputColumns={splitInputColumns}
			{nodeError}
	/>
	{:else}
		<svelte:component
			this={EditorComponent}
			{selectedNode}
			params={childParams}
			onDraft={patchChild}
			onCommit={commitChild}
		/>
	{/if}
{/if}

<style>
	.schemaCard {
		border: 1px solid var(--field-border, #334155);
		border-radius: 8px;
		padding: 8px;
		margin-bottom: 8px;
	}

	.schemaHeader {
		display: flex;
		justify-content: space-between;
		gap: 8px;
		font-size: 12px;
		margin-bottom: 6px;
	}

	.schemaLabel {
		font-weight: 600;
		word-break: break-word;
	}

	.schemaRows {
		opacity: 0.85;
	}

	.schemaProv {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
		font-size: 11px;
		opacity: 0.8;
		margin-bottom: 6px;
	}

	.schemaCols {
		display: grid;
		gap: 4px;
	}

	.schemaCol {
		display: flex;
		justify-content: space-between;
		gap: 8px;
		font-size: 12px;
	}

	.schemaColName {
		min-width: 0;
		overflow-wrap: anywhere;
	}

	.schemaColType {
		opacity: 0.85;
	}

	.schemaMuted {
		font-size: 12px;
		opacity: 0.8;
	}

	.schemaError {
		font-size: 12px;
		color: #f87171;
	}

	.schemaCoercion {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
		font-size: 11px;
		margin-bottom: 6px;
		padding: 6px 8px;
		border: 1px solid var(--field-border, #334155);
		border-radius: 6px;
		background: rgba(148, 163, 184, 0.08);
	}

	.joinHint {
		font-size: 12px;
		opacity: 0.85;
		margin-bottom: 8px;
		padding: 6px 8px;
		border: 1px solid var(--field-border, #334155);
		border-radius: 8px;
	}
</style>
