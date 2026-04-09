<script lang="ts">
	import type { TransformQualityGateParams } from '$lib/flow/schema/transform';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	type Check = Extract<TransformQualityGateParams['checks'][number], { kind: 'leakage' }>;

	export let check: Check;
	export let columnOptions: string[] = [];
	export let onChange: (next: Check) => void;
</script>

<Field label="feature column">
	<ColumnTokenInput
		value={check.featureColumn ? [check.featureColumn] : []}
		schema={columnOptions}
		onChange={(next) => onChange({ ...check, featureColumn: String(next[0] ?? '').trim() })}
		placeholder="Select feature column"
	/>
</Field>
<Field label="target column">
	<ColumnTokenInput
		value={check.targetColumn ? [check.targetColumn] : []}
		schema={columnOptions}
		onChange={(next) => onChange({ ...check, targetColumn: String(next[0] ?? '').trim() })}
		placeholder="Select target column"
	/>
</Field>
<Field label="max abs corr">
	<Input
		type="number"
		min="0"
		max="1"
		step="0.01"
		value={check.maxAbsCorrelation}
		onInput={(e) =>
			onChange({
				...check,
				maxAbsCorrelation: Math.max(0, Math.min(1, Number((e.currentTarget as HTMLInputElement).value || 0.95)))
			})}
	/>
</Field>

