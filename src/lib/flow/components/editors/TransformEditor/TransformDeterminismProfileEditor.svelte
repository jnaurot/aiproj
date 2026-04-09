<script lang="ts">
	import type { TransformDeterminismProfileParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';

	export let params: Partial<TransformDeterminismProfileParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformDeterminismProfileParams>) => void;
	export let onCommit: (patch: Partial<TransformDeterminismProfileParams>) => void;

	const nested = (params as any)?.determinism_profile ?? params ?? {};
	$: strict = Boolean((nested as any)?.strict ?? true);
	$: seed = Number((nested as any)?.seed ?? 42);
	$: stableSort = Boolean((nested as any)?.stableSort ?? true);
	$: stableCoercion = Boolean((nested as any)?.stableCoercion ?? true);
	const meta = getTransformMeta('determinism_profile');

	function commitPatch(next: Partial<TransformDeterminismProfileParams>, immediate = false): void {
		const merged = { strict, seed, stableSort, stableCoercion, ...next };
		onDraft({ op: 'determinism_profile', determinism_profile: merged } as unknown as Partial<TransformDeterminismProfileParams>);
		if (immediate) onCommit({ op: 'determinism_profile', determinism_profile: merged } as unknown as Partial<TransformDeterminismProfileParams>);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="strict">
		<input type="checkbox" checked={strict} on:change={(e) => commitPatch({ strict: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>

	<Field label="seed">
		<Input type="number" step="1" value={seed} onInput={(e) => commitPatch({ seed: Math.trunc(Number((e.currentTarget as HTMLInputElement).value || 0)) })} />
	</Field>

	<Field label="stable sort">
		<input type="checkbox" checked={stableSort} on:change={(e) => commitPatch({ stableSort: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>

	<Field label="stable coercion">
		<input type="checkbox" checked={stableCoercion} on:change={(e) => commitPatch({ stableCoercion: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>
</Section>

