<script lang="ts">
	import type { TransformQualityGateParams } from '$lib/flow/schema/transform';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	type Check = Extract<TransformQualityGateParams['checks'][number], { kind: 'range' }>;

	export let check: Check;
	export let columnOptions: string[] = [];
	export let onChange: (next: Check) => void;

	function parseOptional(raw: string): number | undefined {
		const text = String(raw ?? '').trim();
		if (!text) return undefined;
		const n = Number(text);
		return Number.isFinite(n) ? n : undefined;
	}
</script>

<Field label="column">
	<ColumnTokenInput
		value={check.column ? [check.column] : []}
		schema={columnOptions}
		onChange={(next) => onChange({ ...check, column: String(next[0] ?? '').trim() })}
		placeholder="Select column"
	/>
</Field>
<Field label="min">
	<Input type="number" value={check.min ?? ''} onInput={(e) => onChange({ ...check, min: parseOptional((e.currentTarget as HTMLInputElement).value) })} />
</Field>
<Field label="max">
	<Input type="number" value={check.max ?? ''} onInput={(e) => onChange({ ...check, max: parseOptional((e.currentTarget as HTMLInputElement).value) })} />
</Field>
<Field label="inclusive min">
	<Input type="checkbox" checked={check.inclusiveMin} onChange={(e) => onChange({ ...check, inclusiveMin: (e.currentTarget as HTMLInputElement).checked })} />
</Field>
<Field label="inclusive max">
	<Input type="checkbox" checked={check.inclusiveMax} onChange={(e) => onChange({ ...check, inclusiveMax: (e.currentTarget as HTMLInputElement).checked })} />
</Field>
<Field label="max out-of-range %">
	<Input
		type="number"
		min="0"
		max="1"
		step="0.01"
		value={check.maxOutOfRangePct}
		onInput={(e) =>
			onChange({
				...check,
				maxOutOfRangePct: Math.max(0, Math.min(1, Number((e.currentTarget as HTMLInputElement).value || 0)))
			})}
	/>
</Field>

