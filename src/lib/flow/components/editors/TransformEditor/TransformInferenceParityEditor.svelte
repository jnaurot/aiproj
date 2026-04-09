<script lang="ts">
	import type { TransformInferenceParityParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';

	export let params: Partial<TransformInferenceParityParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformInferenceParityParams>) => void;
	export let onCommit: (patch: Partial<TransformInferenceParityParams>) => void;

	const nested = (params as any)?.inference_parity ?? params ?? {};
	$: trainSignature = String((nested as any)?.trainSignature ?? '');
	$: inferenceSignature = String((nested as any)?.inferenceSignature ?? '');
	$: failOnMismatch = Boolean((nested as any)?.failOnMismatch ?? true);
	const meta = getTransformMeta('inference_parity');

	function commitPatch(next: Partial<TransformInferenceParityParams>, immediate = false): void {
		const merged = { trainSignature, inferenceSignature, failOnMismatch, ...next };
		onDraft({ op: 'inference_parity', inference_parity: merged } as unknown as Partial<TransformInferenceParityParams>);
		if (immediate) onCommit({ op: 'inference_parity', inference_parity: merged } as unknown as Partial<TransformInferenceParityParams>);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="train signature">
		<Input value={trainSignature} onInput={(e) => commitPatch({ trainSignature: (e.currentTarget as HTMLInputElement).value })} />
	</Field>

	<Field label="inference signature">
		<Input value={inferenceSignature} onInput={(e) => commitPatch({ inferenceSignature: (e.currentTarget as HTMLInputElement).value })} />
	</Field>

	<Field label="fail on mismatch">
		<input type="checkbox" checked={failOnMismatch} on:change={(e) => commitPatch({ failOnMismatch: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>
</Section>

