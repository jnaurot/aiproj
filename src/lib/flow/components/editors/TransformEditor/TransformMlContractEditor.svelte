<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformMlContractParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformMlContractParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformMlContractParams>) => void;
	export let onCommit: (patch: Partial<TransformMlContractParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	$: void onCommit;
	$: contractParams = readContractParams(params);
	$: taskType = contractParams.taskType ?? 'other';
	$: labelColumn = String(contractParams.labelColumn ?? 'label');
	$: featureColumns = normalizeFeatureColumns(contractParams.featureColumns);
	$: idColumn = String(contractParams.idColumn ?? '');
	$: timestampColumn = String(contractParams.timestampColumn ?? '');
	$: allowExtraFeatures = contractParams.allowExtraFeatures ?? true;
	$: requireNonNullLabel = contractParams.requireNonNullLabel ?? true;
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;

	function isObject(v: unknown): v is Record<string, unknown> {
		return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
	}

	function readContractParams(raw: unknown): Partial<TransformMlContractParams> {
		if (!isObject(raw)) return {};
		if (isObject(raw.ml_contract)) return raw.ml_contract as Partial<TransformMlContractParams>;
		return raw as Partial<TransformMlContractParams>;
	}

	function isWrappedParams(raw: unknown): boolean {
		return isObject(raw) && ('op' in raw || 'ml_contract' in raw);
	}

	function normalizeFeatureColumns(raw: unknown): string[] {
		if (!Array.isArray(raw)) return ['text'];
		const cols = uniqueStrings(
			raw.map((v) => String(v ?? '').trim()).filter((v) => v.length > 0)
		);
		return cols.length > 0 ? cols : ['text'];
	}

	function commitPatch(next: Partial<TransformMlContractParams>): void {
		const merged = {
			taskType,
			labelColumn: labelColumn.trim() || 'label',
			featureColumns,
			idColumn: idColumn.trim(),
			timestampColumn: timestampColumn.trim(),
			allowExtraFeatures,
			requireNonNullLabel,
			...next
		};
		if (isWrappedParams(params)) {
			onDraft({ op: 'ml_contract', ml_contract: merged } as unknown as Partial<TransformMlContractParams>);
			return;
		}
		onDraft(merged);
	}

	function addFeatureColumn(col: string): void {
		const value = String(col ?? '').trim();
		if (!value) return;
		commitPatch({ featureColumns: uniqueStrings([...featureColumns, value]) });
	}

	function removeFeatureColumn(col: string): void {
		const value = String(col ?? '').trim();
		commitPatch({ featureColumns: featureColumns.filter((c) => c !== value) });
	}
	
</script>

<Section title="ML Contract">
	<div class="hint">Declare required label/features and enforce trainability checks before downstream ML.</div>

	<Field label="task type">
		<select
			value={taskType}
			on:change={(event) =>
				commitPatch({
					taskType: (event.currentTarget as HTMLSelectElement).value as TransformMlContractParams['taskType']
				})}
		>
			<option value="classification">classification</option>
			<option value="regression">regression</option>
			<option value="ranking">ranking</option>
			<option value="generation">generation</option>
			<option value="embedding">embedding</option>
			<option value="pretraining">pretraining</option>
			<option value="finetuning">finetuning</option>
			<option value="other">other</option>
		</select>
	</Field>

	<Field label="label column">
		<Input
			value={labelColumn}
			placeholder="label"
			onInput={(event) => commitPatch({ labelColumn: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	<Field label="id column">
		<Input
			value={idColumn}
			placeholder="id (optional)"
			onInput={(event) => commitPatch({ idColumn: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	<Field label="timestamp column">
		<Input
			value={timestampColumn}
			placeholder="timestamp (optional)"
			onInput={(event) => commitPatch({ timestampColumn: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	<Field label="feature columns">
		<div class="featureControls">
			<select on:change={(event) => addFeatureColumn((event.currentTarget as HTMLSelectElement).value)}>
				<option value="">Select column...</option>
				{#each columnOptions as col (col)}
					<option value={col}>{col}</option>
				{/each}
			</select>
			<Input
				placeholder="Type feature and press Enter"
				onKeydown={(event) => {
					if (event.key !== 'Enter') return;
					event.preventDefault();
					const el = event.currentTarget as HTMLInputElement;
					addFeatureColumn(el.value);
					el.value = '';
				}}
			/>
		</div>
		{#if featureColumns.length > 0}
			<div class="chips">
				{#each featureColumns as col (col)}
					<button type="button" class="chip" on:click={() => removeFeatureColumn(col)}>{col} ×</button>
				{/each}
			</div>
		{/if}
	</Field>

	<Field label="require non-null label">
		<label class="check">
			<input
				type="checkbox"
				checked={requireNonNullLabel}
				on:change={(event) => commitPatch({ requireNonNullLabel: (event.currentTarget as HTMLInputElement).checked })}
			/>
			<span>Fail when label column has nulls.</span>
		</label>
	</Field>

	<Field label="allow extra features">
		<label class="check">
			<input
				type="checkbox"
				checked={allowExtraFeatures}
				on:change={(event) => commitPatch({ allowExtraFeatures: (event.currentTarget as HTMLInputElement).checked })}
			/>
			<span>Allow columns outside the selected feature set.</span>
		</label>
	</Field>
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	.featureControls {
		display: grid;
		gap: 8px;
	}

	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
		margin-top: 8px;
	}

	.chip {
		border: 1px solid rgba(148, 163, 184, 0.4);
		background: rgba(15, 23, 42, 0.45);
		color: #e2e8f0;
		border-radius: 999px;
		padding: 2px 10px;
		font-size: 12px;
		cursor: pointer;
	}

	.check {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 13px;
	}
</style>
