<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceDatabaseParams } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	type SourceDatabasePatch = Partial<SourceDatabaseParams>;

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let params: Partial<SourceDatabaseParams>;
	export let onDraft: (patch: SourceDatabasePatch) => void;
	export let onCommit: (patch: SourceDatabasePatch) => void;

	$: void onCommit;
	$: connection_string = asString(params?.connection_string, '');
	$: connection_ref = asString(params?.connection_ref, '');
	$: query = asString(params?.query, '');
	$: table_name = asString(params?.table_name, '');
	$: limit = asNumberOrEmpty(params?.limit);

	function draft(patch: SourceDatabasePatch): void {
		onDraft?.(patch);
	}
</script>

{#if selectedNode}
	<Section title="Database">
		<Field label="connection_string">
			<Input
				value={connection_string}
				placeholder="postgresql://user:pass@host:5432/db"
				onInput={(event) =>
					draft({ connection_string: (event.currentTarget as HTMLInputElement).value })}
			/>
		</Field>

		<Field label="connection_ref">
			<Input
				value={connection_ref}
				placeholder="(optional) secret/env ref"
				onInput={(event) => {
					const value = (event.currentTarget as HTMLInputElement).value.trim();
					draft({ connection_ref: value === '' ? undefined : value });
				}}
			/>
		</Field>

		<Field label="query">
			<Input
				multiline={true}
				rows={4}
				value={query}
				placeholder="SELECT * FROM table"
				onInput={(event) => {
					const value = (event.currentTarget as HTMLTextAreaElement).value;
					draft({ query: value, table_name: value.trim() ? undefined : params?.table_name });
				}}
			/>
		</Field>

		<Field label="table_name">
			<Input
				value={table_name}
				placeholder="(optional) table"
				onInput={(event) => {
					const value = (event.currentTarget as HTMLInputElement).value.trim();
					draft({ table_name: value === '' ? undefined : value, query: value ? undefined : params?.query });
				}}
			/>
		</Field>

		<Field label="limit">
			<Input
				type="number"
				min="1"
				step="1"
				value={limit}
				placeholder="e.g. 5000"
				onInput={(event) =>
					draft({ limit: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
			/>
		</Field>

		<p class="hint">
			Backend requires: (connection_string OR connection_ref) AND (query OR table_name).
		</p>
	</Section>
{/if}

<style>
	.hint {
		margin-top: 8px;
		font-size: 12px;
		opacity: 0.75;
	}
</style>
