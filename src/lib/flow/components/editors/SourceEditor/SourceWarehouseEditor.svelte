<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceWarehouseParams, SourceOutputMode } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	type SourceWarehousePatch = Partial<SourceWarehouseParams>;
	type Provider = SourceWarehouseParams['provider'];

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let params: Partial<SourceWarehouseParams>;
	export let onDraft: (patch: SourceWarehousePatch) => void;
	export let onCommit: (patch: SourceWarehousePatch) => void;

	const providers: Provider[] = ['snowflake', 'bigquery', 'databricks_sql'];
	const outputModes: SourceOutputMode[] = ['table', 'text', 'json', 'binary'];

	$: provider = (asString(params?.provider, 'snowflake') as Provider) ?? 'snowflake';
	$: connection_string = asString(params?.connection_string, '');
	$: connection_ref = asString(params?.connection_ref, '');
	$: query = asString(params?.query, '');
	$: limit = asNumberOrEmpty(params?.limit);
	$: outputMode = (asString(params?.output?.mode, 'table') as SourceOutputMode) ?? 'table';

	function draft(patch: SourceWarehousePatch): void {
		onDraft?.(patch);
	}

	function commit(patch: SourceWarehousePatch): void {
		onCommit?.(patch);
	}
</script>

{#if selectedNode}
	<Section title="Warehouse">
		<Field label="provider">
			<select
				value={provider}
				on:change={(event) => {
					const value = (event.currentTarget as HTMLSelectElement).value as Provider;
					draft({ provider: value });
					commit({ provider: value });
				}}
			>
				{#each providers as p}
					<option value={p}>{p}</option>
				{/each}
			</select>
		</Field>

		<Field label="connection_string">
			<Input
				value={connection_string}
				placeholder="warehouse connection string"
				onInput={(event) => draft({ connection_string: (event.currentTarget as HTMLInputElement).value })}

			/>
		</Field>

		<Field label="connection_ref">
			<Input
				value={connection_ref}
				placeholder="conn:warehouse_default"
				onInput={(event) => draft({ connection_ref: (event.currentTarget as HTMLInputElement).value })}

			/>
		</Field>

		<Field label="query">
			<Input
				multiline={true}
				rows={5}
				value={query}
				placeholder="select * from my_table"
				onInput={(event) => draft({ query: (event.currentTarget as HTMLTextAreaElement).value })}

			/>
		</Field>

		<Field label="limit">
			<Input
				type="number"
				min="1"
				step="1"
				value={limit}
				onInput={(event) =>
					draft({ limit: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}

			/>
		</Field>

		<Field label="output mode">
			<select
				value={outputMode}
				on:change={(event) => {
					const mode = (event.currentTarget as HTMLSelectElement).value as SourceOutputMode;
					draft({ output: { ...(params?.output ?? {}), mode } });
					commit({ output: { ...(params?.output ?? {}), mode } });
				}}
			>
				{#each outputModes as mode}
					<option value={mode}>{mode}</option>
				{/each}
			</select>
		</Field>
	</Section>
{/if}
