<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformQualityGateParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import QualityCheckNullPct from './QualityCheckNullPct.svelte';
	import QualityCheckRange from './QualityCheckRange.svelte';
	import QualityCheckUniqueness from './QualityCheckUniqueness.svelte';
	import QualityCheckClassBalance from './QualityCheckClassBalance.svelte';
	import QualityCheckLeakage from './QualityCheckLeakage.svelte';

	type GateCheck = TransformQualityGateParams['checks'][number];
	type GateSeverity = Extract<GateCheck, { severity: string }>['severity'];
	type GateKind = GateCheck['kind'];

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformQualityGateParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformQualityGateParams>) => void;
	export let onCommit: (patch: Partial<TransformQualityGateParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	$: gateParams = readGateParams(params);
	$: checks = normalizeChecks(gateParams.checks);
	$: stopOnFail = gateParams.stopOnFail ?? true;
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;
	let pendingKind: GateKind = 'null_pct';
	const meta = getTransformMeta('quality_gate');

	function isObject(v: unknown): v is Record<string, unknown> {
		return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
	}

	function readGateParams(raw: unknown): Partial<TransformQualityGateParams> {
		if (!isObject(raw)) return {};
		if (isObject(raw.quality_gate)) return raw.quality_gate as Partial<TransformQualityGateParams>;
		return raw as Partial<TransformQualityGateParams>;
	}

	function isWrappedParams(raw: unknown): boolean {
		return isObject(raw) && ('op' in raw || 'quality_gate' in raw);
	}

	function normalizeChecks(raw: unknown): GateCheck[] {
		if (!Array.isArray(raw)) return [];
		const out: GateCheck[] = [];
		for (const item of raw) {
			if (!isObject(item)) continue;
			const severity = (String(item.severity ?? 'fail').trim().toLowerCase() === 'warn' ? 'warn' : 'fail') as GateSeverity;
			const kind = String(item.kind ?? '').trim().toLowerCase() as GateKind;
			if (kind === 'null_pct') {
				out.push({ kind, column: String(item.column ?? ''), maxNullPct: ratio(item.maxNullPct, 0), severity });
				continue;
			}
			if (kind === 'range') {
				out.push({
					kind,
					column: String(item.column ?? ''),
					min: optionalNumber(item.min),
					max: optionalNumber(item.max),
					inclusiveMin: bool(item.inclusiveMin, true),
					inclusiveMax: bool(item.inclusiveMax, true),
					maxOutOfRangePct: ratio(item.maxOutOfRangePct, 0),
					severity
				});
				continue;
			}
			if (kind === 'uniqueness') {
				out.push({ kind, column: String(item.column ?? ''), minUniqueRatio: ratio(item.minUniqueRatio, 1), severity });
				continue;
			}
			if (kind === 'class_balance') {
				out.push({
					kind,
					column: String(item.column ?? ''),
					minMinorityRatio: ratio(item.minMinorityRatio, 0),
					maxDominantRatio: ratio(item.maxDominantRatio, 1),
					severity
				});
				continue;
			}
			if (kind === 'leakage') {
				out.push({
					kind,
					featureColumn: String(item.featureColumn ?? ''),
					targetColumn: String(item.targetColumn ?? ''),
					maxAbsCorrelation: ratio(item.maxAbsCorrelation, 0.95),
					severity
				});
			}
		}
		return out;
	}

	function ratio(value: unknown, fallback: number): number {
		const n = Number(value);
		if (!Number.isFinite(n)) return fallback;
		return Math.max(0, Math.min(1, n));
	}

	function bool(value: unknown, fallback: boolean): boolean {
		return typeof value === 'boolean' ? value : fallback;
	}

	function optionalNumber(value: unknown): number | undefined {
		const text = String(value ?? '').trim();
		if (!text) return undefined;
		const n = Number(text);
		return Number.isFinite(n) ? n : undefined;
	}

	function defaultCheck(kind: GateKind): GateCheck {
		if (kind === 'null_pct') return { kind, column: 'text', maxNullPct: 0, severity: 'fail' };
		if (kind === 'range') {
			return {
				kind,
				column: 'value',
				min: 0,
				max: 1,
				inclusiveMin: true,
				inclusiveMax: true,
				maxOutOfRangePct: 0,
				severity: 'fail'
			};
		}
		if (kind === 'uniqueness') return { kind, column: 'id', minUniqueRatio: 1, severity: 'fail' };
		if (kind === 'class_balance') return { kind, column: 'label', minMinorityRatio: 0.1, maxDominantRatio: 0.9, severity: 'warn' };
		return { kind, featureColumn: 'feature', targetColumn: 'target', maxAbsCorrelation: 0.95, severity: 'warn' };
	}

	function patchDraft(next: Partial<TransformQualityGateParams>): void {
		const merged = { checks, stopOnFail, ...next };
		if (isWrappedParams(params)) {
			onDraft({ op: 'quality_gate', quality_gate: merged } as unknown as Partial<TransformQualityGateParams>);
			return;
		}
		onDraft(merged);
	}

	function patchCommit(next: Partial<TransformQualityGateParams>): void {
		const merged = { checks, stopOnFail, ...next };
		if (isWrappedParams(params)) {
			onCommit({ op: 'quality_gate', quality_gate: merged } as unknown as Partial<TransformQualityGateParams>);
			return;
		}
		onCommit(merged);
	}

	function replaceChecks(nextChecks: GateCheck[], immediate = true): void {
		patchDraft({ checks: nextChecks });
		if (immediate) patchCommit({ checks: nextChecks });
	}

	function addCheck(kind: GateKind): void {
		replaceChecks([...checks, defaultCheck(kind)], true);
	}

	function removeCheck(index: number): void {
		replaceChecks(checks.filter((_, i) => i !== index), true);
	}

	function updateCheck(index: number, next: GateCheck, immediate = false): void {
		replaceChecks(checks.map((check, i) => (i === index ? next : check)), immediate);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="stop on fail">
		<Input
			type="checkbox"
			checked={stopOnFail}
			onChange={(event) => {
				const value = (event.currentTarget as HTMLInputElement).checked;
				patchDraft({ stopOnFail: value });
				patchCommit({ stopOnFail: value });
			}}
		/>
	</Field>

	<div class="newCheckRow">
		<select bind:value={pendingKind}>
			<option value="null_pct">null %</option>
			<option value="range">range</option>
			<option value="uniqueness">uniqueness</option>
			<option value="class_balance">class balance</option>
			<option value="leakage">leakage</option>
		</select>
		<button class="small ghost" type="button" on:click={() => addCheck(pendingKind)}>Add Check</button>
	</div>

	{#if checks.length === 0}
		<div class="empty">No checks configured.</div>
	{/if}

	{#each checks as check, index (index)}
		<div class="checkCard">
			<div class="checkHead">
				<div class="checkTitle">#{index + 1} {check.kind}</div>
				<button class="small ghost" type="button" on:click={() => removeCheck(index)}>Remove</button>
			</div>

			<Field label="severity">
				<select
					value={check.severity}
					on:change={(event) =>
						updateCheck(index, { ...check, severity: (event.currentTarget as HTMLSelectElement).value as GateSeverity }, true)}
				>
					<option value="fail">fail</option>
					<option value="warn">warn</option>
				</select>
			</Field>

			{#if check.kind === 'null_pct'}
				<QualityCheckNullPct check={check} {columnOptions} onChange={(next) => updateCheck(index, next, false)} />
			{:else if check.kind === 'range'}
				<QualityCheckRange check={check} {columnOptions} onChange={(next) => updateCheck(index, next, false)} />
			{:else if check.kind === 'uniqueness'}
				<QualityCheckUniqueness check={check} {columnOptions} onChange={(next) => updateCheck(index, next, false)} />
			{:else if check.kind === 'class_balance'}
				<QualityCheckClassBalance check={check} {columnOptions} onChange={(next) => updateCheck(index, next, false)} />
			{:else if check.kind === 'leakage'}
				<QualityCheckLeakage check={check} {columnOptions} onChange={(next) => updateCheck(index, next, false)} />
			{/if}
		</div>
	{/each}
</Section>

<style>
	.newCheckRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 8px;
		margin-bottom: 10px;
	}

	.checkCard {
		border: 1px solid var(--ni-border, rgba(255, 255, 255, 0.12));
		border-radius: 10px;
		padding: 10px;
		margin-bottom: 10px;
	}

	.checkHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		margin-bottom: 8px;
	}

	.checkTitle {
		font-size: 13px;
		font-weight: 650;
		text-transform: lowercase;
	}

	.empty {
		opacity: 0.75;
		font-size: 12px;
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

	.small {
		padding: 6px 10px;
		border-radius: 8px;
		border: 1px solid var(--ni-control-border, rgba(255, 255, 255, 0.15));
		background: transparent;
		color: inherit;
	}
</style>
