<script lang="ts">
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: any;
	export let params: Record<string, unknown>;
	export let onDraft: (patch: Record<string, unknown>) => void;
	export let onCommit: (patch: Record<string, unknown>) => void;

	function isObject(v: unknown): v is Record<string, unknown> {
		return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
	}

	$: op = String((params as any)?.op ?? (selectedNode?.data as any)?.transformKind ?? '').trim();
	$: nested = isObject((params as any)?.[op]) ? ((params as any)[op] as Record<string, unknown>) : {};
	$: columnsText = columnsToText(
		op === 'leakage_detect'
			? nested.keyColumns
			: op === 'quality_profile'
				? nested.columns
				: op === 'drift_compare'
					? nested.compareColumns
					: op === 'fit_state_registry'
						? nested.includeColumns
						: op === 'pii_guard'
							? nested.columns
							: []
	);

	function parseColumns(raw: string): string[] {
		return raw
			.split(',')
			.map((v) => v.trim())
			.filter((v) => v.length > 0);
	}

	function columnsToText(raw: unknown): string {
		return Array.isArray(raw) ? raw.map((v) => String(v ?? '').trim()).filter((v) => v.length > 0).join(', ') : '';
	}

	function toNumber(raw: string, fallback: number): number {
		const n = Number(raw);
		return Number.isFinite(n) ? n : fallback;
	}

	function commitPatch(nextNested: Record<string, unknown>, immediate = false): void {
		const patch = { op, [op]: { ...nested, ...nextNested } };
		onDraft(patch);
		if (immediate) onCommit(patch);
	}
</script>

<Section title={`ML-Pre Advanced: ${op || 'transform'}`}>
	{#if op === 'leakage_detect'}
		<Field label="split column">
			<Input
				value={String(nested.splitColumn ?? 'split')}
				onInput={(e) => commitPatch({ splitColumn: (e.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
		<Field label="key columns (comma-separated)">
			<Input
				value={columnsText}
				onInput={(e) => commitPatch({ keyColumns: parseColumns((e.currentTarget as HTMLInputElement).value) })}
			/>
		</Field>
		<Field label="label column">
			<Input
				value={String(nested.labelColumn ?? '')}
				onInput={(e) => commitPatch({ labelColumn: (e.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
		<Field label="max allowed overlap (0-1)">
			<Input
				value={String(nested.maxAllowedOverlap ?? 0)}
				onInput={(e) => commitPatch({ maxAllowedOverlap: toNumber((e.currentTarget as HTMLInputElement).value, 0) })}
			/>
		</Field>
	{:else if op === 'quality_profile'}
		<Field label="columns (comma-separated)">
			<Input
				value={columnsText}
				onInput={(e) => commitPatch({ columns: parseColumns((e.currentTarget as HTMLInputElement).value) })}
			/>
		</Field>
		<Field label="include histograms">
			<Input
				type="checkbox"
				checked={Boolean(nested.includeHistograms ?? true)}
				onChange={(e) => commitPatch({ includeHistograms: (e.currentTarget as HTMLInputElement).checked }, true)}
			/>
		</Field>
		<Field label="include samples">
			<Input
				type="checkbox"
				checked={Boolean(nested.includeSamples ?? true)}
				onChange={(e) => commitPatch({ includeSamples: (e.currentTarget as HTMLInputElement).checked }, true)}
			/>
		</Field>
	{:else if op === 'drift_compare'}
		<Field label="baseline ref">
			<Input
				value={String(nested.baselineRef ?? '')}
				onInput={(e) => commitPatch({ baselineRef: (e.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
		<Field label="compare columns (comma-separated)">
			<Input
				value={columnsText}
				onInput={(e) => commitPatch({ compareColumns: parseColumns((e.currentTarget as HTMLInputElement).value) })}
			/>
		</Field>
		<Field label="metric">
			<select
				value={String(nested.metric ?? 'psi')}
				on:change={(e) => commitPatch({ metric: (e.currentTarget as HTMLSelectElement).value }, true)}
			>
				<option value="psi">psi</option>
				<option value="jsd">jsd</option>
				<option value="ks">ks</option>
			</select>
		</Field>
		<Field label="threshold">
			<Input
				value={String(nested.threshold ?? 0.2)}
				onInput={(e) => commitPatch({ threshold: toNumber((e.currentTarget as HTMLInputElement).value, 0.2) })}
			/>
		</Field>
		<Field label="fail on drift">
			<Input
				type="checkbox"
				checked={Boolean(nested.failOnDrift ?? false)}
				onChange={(e) => commitPatch({ failOnDrift: (e.currentTarget as HTMLInputElement).checked }, true)}
			/>
		</Field>
	{:else if op === 'determinism_profile'}
		<Field label="strict">
			<Input
				type="checkbox"
				checked={Boolean(nested.strict ?? true)}
				onChange={(e) => commitPatch({ strict: (e.currentTarget as HTMLInputElement).checked }, true)}
			/>
		</Field>
		<Field label="seed">
			<Input
				value={String(nested.seed ?? 42)}
				onInput={(e) => commitPatch({ seed: Math.trunc(toNumber((e.currentTarget as HTMLInputElement).value, 42)) })}
			/>
		</Field>
		<Field label="stable sort">
			<Input
				type="checkbox"
				checked={Boolean(nested.stableSort ?? true)}
				onChange={(e) => commitPatch({ stableSort: (e.currentTarget as HTMLInputElement).checked }, true)}
			/>
		</Field>
		<Field label="stable coercion">
			<Input
				type="checkbox"
				checked={Boolean(nested.stableCoercion ?? true)}
				onChange={(e) => commitPatch({ stableCoercion: (e.currentTarget as HTMLInputElement).checked }, true)}
			/>
		</Field>
	{:else if op === 'fit_state_registry'}
		<Field label="mode">
			<select
				value={String(nested.mode ?? 'fit')}
				on:change={(e) => commitPatch({ mode: (e.currentTarget as HTMLSelectElement).value }, true)}
			>
				<option value="fit">fit</option>
				<option value="apply">apply</option>
			</select>
		</Field>
		<Field label="state key">
			<Input
				value={String(nested.stateKey ?? 'default')}
				onInput={(e) => commitPatch({ stateKey: (e.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
		<Field label="include columns (comma-separated)">
			<Input
				value={columnsText}
				onInput={(e) => commitPatch({ includeColumns: parseColumns((e.currentTarget as HTMLInputElement).value) })}
			/>
		</Field>
	{:else if op === 'pii_guard'}
		<Field label="columns (comma-separated)">
			<Input
				value={columnsText}
				onInput={(e) => commitPatch({ columns: parseColumns((e.currentTarget as HTMLInputElement).value) })}
			/>
		</Field>
		<Field label="action">
			<select
				value={String(nested.action ?? 'report')}
				on:change={(e) => commitPatch({ action: (e.currentTarget as HTMLSelectElement).value }, true)}
			>
				<option value="report">report</option>
				<option value="mask">mask</option>
				<option value="drop_rows">drop_rows</option>
			</select>
		</Field>
		<Field label="fail on detect">
			<Input
				type="checkbox"
				checked={Boolean(nested.failOnDetect ?? false)}
				onChange={(e) => commitPatch({ failOnDetect: (e.currentTarget as HTMLInputElement).checked }, true)}
			/>
		</Field>
	{:else if op === 'inference_parity'}
		<Field label="train signature">
			<Input
				value={String(nested.trainSignature ?? '')}
				onInput={(e) => commitPatch({ trainSignature: (e.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
		<Field label="inference signature">
			<Input
				value={String(nested.inferenceSignature ?? '')}
				onInput={(e) => commitPatch({ inferenceSignature: (e.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
		<Field label="fail on mismatch">
			<Input
				type="checkbox"
				checked={Boolean(nested.failOnMismatch ?? true)}
				onChange={(e) => commitPatch({ failOnMismatch: (e.currentTarget as HTMLInputElement).checked }, true)}
			/>
		</Field>
	{:else}
		<div class="hint">No dedicated controls available for this transform yet.</div>
	{/if}
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	select {
		width: 100%;
		min-height: 38px;
		border-radius: 10px;
		padding: 6px 10px;
		background: var(--ni-control-bg, rgba(0, 0, 0, 0.2));
		color: var(--ni-control-text, inherit);
		border: 1px solid var(--ni-control-border, rgba(255, 255, 255, 0.15));
	}
</style>
