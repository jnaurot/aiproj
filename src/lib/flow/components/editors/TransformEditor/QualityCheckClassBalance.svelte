<script lang="ts">
	import type { TransformQualityGateParams } from '$lib/flow/schema/transform';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	type Check = Extract<TransformQualityGateParams['checks'][number], { kind: 'class_balance' }>;

	export let check: Check;
	export let columnOptions: string[] = [];
	export let onChange: (next: Check) => void;
</script>

<Field label="column">
	<ColumnTokenInput
		value={check.column ? [check.column] : []}
		schema={columnOptions}
		onChange={(next) => onChange({ ...check, column: String(next[0] ?? '').trim() })}
		placeholder="Select label column"
	/>
</Field>
<Field label="min minority ratio">
	<Input
		type="number"
		min="0"
		max="1"
		step="0.01"
		value={check.minMinorityRatio}
		onInput={(e) =>
			onChange({
				...check,
				minMinorityRatio: Math.max(0, Math.min(1, Number((e.currentTarget as HTMLInputElement).value || 0)))
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
		onInput={(e) =>
			onChange({
				...check,
				maxDominantRatio: Math.max(0, Math.min(1, Number((e.currentTarget as HTMLInputElement).value || 1)))
			})}
	/>
</Field>

