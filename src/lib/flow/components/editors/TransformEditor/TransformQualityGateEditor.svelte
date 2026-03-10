<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformQualityGateParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

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
			const severity = (String(item.severity ?? 'fail').trim().toLowerCase() === 'warn'
				? 'warn'
				: 'fail') as GateSeverity;
			const kind = String(item.kind ?? '').trim().toLowerCase() as GateKind;
			if (kind === 'null_pct') {
				out.push({
					kind,
					column: String(item.column ?? ''),
					maxNullPct: toRatio(item.maxNullPct, 0),
					severity
				});
				continue;
			}
			if (kind === 'range') {
				const minValue = parseOptionalNumber(item.min);
				const maxValue = parseOptionalNumber(item.max);
				if (minValue === undefined && maxValue === undefined) continue;
				out.push({
					kind,
					column: String(item.column ?? ''),
					min: minValue,
					max: maxValue,
					inclusiveMin: toBool(item.inclusiveMin, true),
					inclusiveMax: toBool(item.inclusiveMax, true),
					maxOutOfRangePct: toRatio(item.maxOutOfRangePct, 0),
					severity
				});
				continue;
			}
			if (kind === 'uniqueness') {
				out.push({
					kind,
					column: String(item.column ?? ''),
					minUniqueRatio: toRatio(item.minUniqueRatio, 1),
					severity
				});
				continue;
			}
			if (kind === 'class_balance') {
				out.push({
					kind,
					column: String(item.column ?? ''),
					minMinorityRatio: toRatio(item.minMinorityRatio, 0),
					maxDominantRatio: toRatio(item.maxDominantRatio, 1),
					severity
				});
				continue;
			}
			if (kind === 'leakage') {
				out.push({
					kind,
					featureColumn: String(item.featureColumn ?? ''),
					targetColumn: String(item.targetColumn ?? ''),
					maxAbsCorrelation: toRatio(item.maxAbsCorrelation, 0.95),
					severity
				});
			}
		}
		return out;
	}

	function toRatio(value: unknown, fallback: number): number {
		const n = Number(value);
		if (!Number.isFinite(n)) return fallback;
		return Math.max(0, Math.min(1, n));
	}

	function toBool(value: unknown, fallback: boolean): boolean {
		return typeof value === 'boolean' ? value : fallback;
	}

	function parseOptionalNumber(value: unknown): number | undefined {
		const text = String(value ?? '').trim();
		if (!text) return undefined;
		const n = Number(text);
		return Number.isFinite(n) ? n : undefined;
	}

	function defaultCheck(kind: GateKind): GateCheck {
		if (kind === 'null_pct') {
			return { kind, column: 'text', maxNullPct: 0, severity: 'fail' };
		}
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
		if (kind === 'uniqueness') {
			return { kind, column: 'id', minUniqueRatio: 1, severity: 'fail' };
		}
		if (kind === 'class_balance') {
			return { kind, column: 'label', minMinorityRatio: 0.1, maxDominantRatio: 0.9, severity: 'warn' };
		}
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

	function replaceChecks(nextChecks: GateCheck[]) {
		patchDraft({ checks: nextChecks });
		patchCommit({ checks: nextChecks });
	}

	function addCheck(kind: GateKind) {
		replaceChecks([...checks, defaultCheck(kind)]);
	}

	function removeCheck(index: number) {
		replaceChecks(checks.filter((_, i) => i !== index));
	}

	function updateCheck(index: number, next: Partial<GateCheck>) {
		const updated = checks.map((check, i) => {
			if (i !== index) return check;
			return { ...check, ...next } as GateCheck;
		});
		replaceChecks(updated);
	}

	function updateRangeBound(index: number, side: 'min' | 'max', rawValue: string) {
		const value = parseOptionalNumber(rawValue);
		const updated = checks.map((check, i) => {
			if (i !== index || check.kind !== 'range') return check;
			if (value === undefined) {
				const nextCheck = { ...check };
				delete (nextCheck as Record<string, unknown>)[side];
				return nextCheck;
			}
			return { ...check, [side]: value };
		});
		replaceChecks(updated);
	}
</script>

<Section title="Quality Gate">
	<div class="hint">Validate table quality before downstream execution.</div>

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

	{#if columnOptions.length > 0}
		<div class="hint">Available columns: {columnOptions.join(', ')}</div>
	{/if}

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
						updateCheck(index, { severity: (event.currentTarget as HTMLSelectElement).value as GateSeverity })}
				>
					<option value="fail">fail</option>
					<option value="warn">warn</option>
				</select>
			</Field>

			{#if check.kind === 'null_pct'}
				<Field label="column">
					<Input
						value={check.column}
						onInput={(event) =>
							updateCheck(index, { column: (event.currentTarget as HTMLInputElement).value })}
					/>
				</Field>
				<Field label="max null %">
					<Input
						type="number"
						min="0"
						max="1"
						step="0.01"
						value={check.maxNullPct}
						onInput={(event) =>
							updateCheck(index, {
								maxNullPct: toRatio((event.currentTarget as HTMLInputElement).value, check.maxNullPct)
							})}
					/>
				</Field>
			{:else if check.kind === 'range'}
				<Field label="column">
					<Input
						value={check.column}
						onInput={(event) =>
							updateCheck(index, { column: (event.currentTarget as HTMLInputElement).value })}
					/>
				</Field>
				<Field label="min">
					<Input
						type="number"
						value={check.min ?? ''}
						onInput={(event) => updateRangeBound(index, 'min', (event.currentTarget as HTMLInputElement).value)}
					/>
				</Field>
				<Field label="max">
					<Input
						type="number"
						value={check.max ?? ''}
						onInput={(event) => updateRangeBound(index, 'max', (event.currentTarget as HTMLInputElement).value)}
					/>
				</Field>
				<Field label="inclusive min">
					<Input
						type="checkbox"
						checked={check.inclusiveMin}
						onChange={(event) =>
							updateCheck(index, { inclusiveMin: (event.currentTarget as HTMLInputElement).checked })}
					/>
				</Field>
				<Field label="inclusive max">
					<Input
						type="checkbox"
						checked={check.inclusiveMax}
						onChange={(event) =>
							updateCheck(index, { inclusiveMax: (event.currentTarget as HTMLInputElement).checked })}
					/>
				</Field>
				<Field label="max out-of-range %">
					<Input
						type="number"
						min="0"
						max="1"
						step="0.01"
						value={check.maxOutOfRangePct}
						onInput={(event) =>
							updateCheck(index, {
								maxOutOfRangePct: toRatio((event.currentTarget as HTMLInputElement).value, check.maxOutOfRangePct)
							})}
					/>
				</Field>
			{:else if check.kind === 'uniqueness'}
				<Field label="column">
					<Input
						value={check.column}
						onInput={(event) =>
							updateCheck(index, { column: (event.currentTarget as HTMLInputElement).value })}
					/>
				</Field>
				<Field label="min unique ratio">
					<Input
						type="number"
						min="0"
						max="1"
						step="0.01"
						value={check.minUniqueRatio}
						onInput={(event) =>
							updateCheck(index, {
								minUniqueRatio: toRatio((event.currentTarget as HTMLInputElement).value, check.minUniqueRatio)
							})}
					/>
				</Field>
			{:else if check.kind === 'class_balance'}
				<Field label="column">
					<Input
						value={check.column}
						onInput={(event) =>
							updateCheck(index, { column: (event.currentTarget as HTMLInputElement).value })}
					/>
				</Field>
				<Field label="min minority ratio">
					<Input
						type="number"
						min="0"
						max="1"
						step="0.01"
						value={check.minMinorityRatio}
						onInput={(event) =>
							updateCheck(index, {
								minMinorityRatio: toRatio((event.currentTarget as HTMLInputElement).value, check.minMinorityRatio)
							})}
					/>
				</Field>
				<Field label="max dominant ratio">
					<Input
						type="number"
						min="0"
						max="1"
						step="0.01"
						value={check.maxDominantRatio}
						onInput={(event) =>
							updateCheck(index, {
								maxDominantRatio: toRatio((event.currentTarget as HTMLInputElement).value, check.maxDominantRatio)
							})}
					/>
				</Field>
			{:else if check.kind === 'leakage'}
				<Field label="feature column">
					<Input
						value={check.featureColumn}
						onInput={(event) =>
							updateCheck(index, { featureColumn: (event.currentTarget as HTMLInputElement).value })}
					/>
				</Field>
				<Field label="target column">
					<Input
						value={check.targetColumn}
						onInput={(event) =>
							updateCheck(index, { targetColumn: (event.currentTarget as HTMLInputElement).value })}
					/>
				</Field>
				<Field label="max abs corr">
					<Input
						type="number"
						min="0"
						max="1"
						step="0.01"
						value={check.maxAbsCorrelation}
						onInput={(event) =>
							updateCheck(index, {
								maxAbsCorrelation: toRatio((event.currentTarget as HTMLInputElement).value, check.maxAbsCorrelation)
							})}
					/>
				</Field>
			{/if}
		</div>
	{/each}

</Section>

<style>
	.hint {
		opacity: 0.8;
		font-size: 12px;
		margin-bottom: 10px;
	}

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
