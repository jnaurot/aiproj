<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformLimitParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { parseOptionalInt } from '$lib/flow/components/editors/shared';
	import { graphStore } from '$lib/flow/store/graphStore';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformLimitParams>;
	export let onDraft: (patch: Partial<TransformLimitParams>) => void;
	export let onCommit: (patch: Partial<TransformLimitParams>) => void;

	const initialDefaultValue = 100;
	const minValue = 1;
	const maxValue = 1_000_000;
	let nDraft = String(initialDefaultValue);
	let lastNodeId = '';
	let lastStoreN = initialDefaultValue;

	function toFiniteInt(value: unknown): number | undefined {
		if (typeof value === 'number' && Number.isFinite(value)) return Math.trunc(value);
		if (typeof value === 'string') {
			const parsed = Number.parseInt(value, 10);
			if (Number.isFinite(parsed)) return parsed;
		}
		return undefined;
	}

	$: void selectedNode?.id;
	$: void params;
	$: persistedN = (() => {
		const nodeId = selectedNode?.id;
		if (!nodeId) return undefined;
		const node = $graphStore.nodes.find((n) => n.id === nodeId);
		const p = (node?.data?.params ?? {}) as Record<string, unknown>;
		const nested = p.limit as Record<string, unknown> | undefined;
		return toFiniteInt(nested?.n) ?? toFiniteInt(p.n);
	})();
	$: n = persistedN ?? initialDefaultValue;
	$: nSafe = Math.max(minValue, Math.min(maxValue, Math.trunc(n)));

	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		nDraft = String(nSafe);
		lastStoreN = nSafe;
	}

	$: if ((selectedNode?.id ?? '') === lastNodeId && nSafe !== lastStoreN) {
		// Keep local input in sync only when store value changed externally.
		nDraft = String(nSafe);
		lastStoreN = nSafe;
	}

	function commitValue(raw: string, fallback = nSafe): void {
		const parsed = parseOptionalInt(raw, minValue);
		const next = parsed === undefined ? fallback : Math.min(maxValue, parsed);
		nDraft = String(next);
		onDraft({ n: next });
		onCommit({ n: next });
	}
</script>

<Section title="Limit">
	<div class="hint">Return only the first <code>n</code> rows.</div>

	<Field >
		<div class="row">
			<Input
				type="number"
				min={String(minValue)}
				max={String(maxValue)}
				step="1"
				value={nDraft}
				onInput={(event) => {
					const raw = (event.currentTarget as HTMLInputElement).value;
					nDraft = raw;
					const parsed = parseOptionalInt(raw, minValue);
					if (parsed !== undefined) onDraft({ n: Math.min(maxValue, parsed) });
				}}
				onBlur={() => commitValue(nDraft, nSafe)}
				onKeydown={(event) => {
					if ((event as KeyboardEvent).key === 'Enter') {
						event.preventDefault();
						commitValue(nDraft, nSafe);
					}
				}}
			/>
			<!-- <button
				class="small ghost"
				type="button"
				on:click={() => commitValue(String(nSafe), nSafe)}
			>
				Reset
			</button> -->
		</div>
	</Field>
</Section>

<style>
	.row {
		display: flex;
		gap: 8px;
		align-items: center;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	button.small {
		padding: 6px 10px;
		font-size: 12px;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.06);
		color: inherit;
		cursor: pointer;
	}

	button.ghost {
		background: transparent;
	}

	code {
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 12px;
	}
</style>
