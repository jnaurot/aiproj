<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceKind } from '$lib/flow/types/paramsMap';
	import type { SourceOutputMode } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ThemedSelect, { type ThemedSelectOption } from '$lib/flow/components/ui/ThemedSelect.svelte';
	import SourceCapabilityBanner from './SourceCapabilityBanner.svelte';
	import { asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let sourceKind: SourceKind = 'file';
	export let params: Record<string, unknown> = {};
	export let nodeError: Record<string, unknown> | null = null;
	export let onDraft: (patch: Record<string, unknown>) => void;
	export let onCommit: (patch: Record<string, unknown>) => void;

	const outputModes: SourceOutputMode[] = ['table', 'text', 'json', 'binary'];
	const outputModeOptions: ThemedSelectOption[] = outputModes.map((value) => ({ value, label: value }));
	const boolOptions: ThemedSelectOption[] = [
		{ value: 'true', label: 'true' },
		{ value: 'false', label: 'false' }
	];
	const methodOptions: ThemedSelectOption[] = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD'].map((value) => ({
		value,
		label: value
	}));
	const authTypeOptions: ThemedSelectOption[] = ['none', 'bearer', 'basic', 'api_key'].map((value) => ({
		value,
		label: value
	}));
	const objectProviderOptions: ThemedSelectOption[] = ['s3', 'azure_blob', 'gcs'].map((value) => ({
		value,
		label: value
	}));
	const warehouseProviderOptions: ThemedSelectOption[] = ['snowflake', 'bigquery', 'databricks_sql'].map((value) => ({
		value,
		label: value
	}));
	$: void nodeError;

	function draft(patch: Record<string, unknown>): void {
		onDraft?.(patch);
	}

	function commit(patch: Record<string, unknown>): void {
		onCommit?.(patch);
	}
</script>

{#if selectedNode}
	<Section title="Guided Source Setup">
		<SourceCapabilityBanner sourceKind={sourceKind} params={params as Record<string, unknown>} />
		{#if sourceKind === 'file'}
			<Field label="filename">
				<Input
					value={asString((params as any)?.filename, '')}
					placeholder="data.txt"
					onInput={(event) => draft({ filename: (event.currentTarget as HTMLInputElement).value })}

				/>
			</Field>
			<Field label="file_format">
				<Input
					value={asString((params as any)?.file_format, 'txt')}
					placeholder="txt/csv/json/..."
					onInput={(event) => draft({ file_format: (event.currentTarget as HTMLInputElement).value })}

				/>
			</Field>
			<Field label="output mode">
				<ThemedSelect
					value={asString((params as any)?.output?.mode, 'text')}
					options={outputModeOptions}
					ariaLabel="guided file output mode"
					onValueChange={(next) => {
						const mode = String(next) as SourceOutputMode;
						draft({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
						commit({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
					}}
				/>
			</Field>
			<Field label="cache_enabled">
				<ThemedSelect
					value={String(Boolean((params as any)?.cache_enabled ?? true))}
					options={boolOptions}
					ariaLabel="guided file cache enabled"
					onValueChange={(next) => {
						const enabled = String(next) === 'true';
						draft({ cache_enabled: enabled });
						commit({ cache_enabled: enabled });
					}}
				/>
			</Field>
		{:else if sourceKind === 'database'}
			<Field label="connection_ref">
				<Input
					value={asString((params as any)?.connection_ref, '')}
					placeholder="conn:default"
					onInput={(event) => draft({ connection_ref: (event.currentTarget as HTMLInputElement).value })}

				/>
			</Field>
			<Field label="query">
				<Input
					multiline={true}
					rows={4}
					value={asString((params as any)?.query, '')}
					placeholder="select * from my_table"
					onInput={(event) => draft({ query: (event.currentTarget as HTMLTextAreaElement).value })}

				/>
			</Field>
			<Field label="limit">
				<Input
					type="number"
					min="1"
					step="1"
					value={asNumberOrEmpty((params as any)?.limit)}
					onInput={(event) => draft({ limit: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}

				/>
			</Field>
			<Field label="output mode">
				<ThemedSelect
					value={asString((params as any)?.output?.mode, 'table')}
					options={outputModeOptions}
					ariaLabel="guided database output mode"
					onValueChange={(next) => {
						const mode = String(next) as SourceOutputMode;
						draft({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
						commit({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
					}}
				/>
			</Field>
		{:else if sourceKind === 'api'}
			<Field label="method">
				<ThemedSelect
					value={asString((params as any)?.method, 'GET')}
					options={methodOptions}
					ariaLabel="guided api method"
					onValueChange={(method) => {
						draft({ method });
						commit({ method });
					}}
				/>
			</Field>
			<Field label="url">
				<Input
					value={asString((params as any)?.url, '')}
					placeholder="https://api.example.com/data"
					onInput={(event) => draft({ url: (event.currentTarget as HTMLInputElement).value })}

				/>
			</Field>
			<Field label="auth_type">
				<ThemedSelect
					value={asString((params as any)?.auth_type, 'none')}
					options={authTypeOptions}
					ariaLabel="guided api auth type"
					onValueChange={(auth_type) => {
						draft({ auth_type });
						commit({ auth_type });
					}}
				/>
			</Field>
			<Field label="auth_token_ref">
				<Input
					value={asString((params as any)?.auth_token_ref, '')}
					placeholder="API_TOKEN"
					onInput={(event) => draft({ auth_token_ref: (event.currentTarget as HTMLInputElement).value })}

				/>
			</Field>
			<Field label="output mode">
				<ThemedSelect
					value={asString((params as any)?.output?.mode, 'json')}
					options={outputModeOptions}
					ariaLabel="guided api output mode"
					onValueChange={(next) => {
						const mode = String(next) as SourceOutputMode;
						draft({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
						commit({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
					}}
				/>
			</Field>
		{:else if sourceKind === 'object_store'}
			<Field label="provider">
				<ThemedSelect
					value={asString((params as any)?.provider, 's3')}
					options={objectProviderOptions}
					ariaLabel="guided object store provider"
					onValueChange={(provider) => {
						draft({ provider });
						commit({ provider });
					}}
				/>
			</Field>
			<Field label="bucket">
				<Input
					value={asString((params as any)?.bucket, '')}
					placeholder="my-bucket"
					onInput={(event) => draft({ bucket: (event.currentTarget as HTMLInputElement).value })}

				/>
			</Field>
			<Field label="key">
				<Input
					value={asString((params as any)?.key, '')}
					placeholder="path/to/object"
					onInput={(event) => draft({ key: (event.currentTarget as HTMLInputElement).value })}

				/>
			</Field>
			<Field label="file_format">
				<Input
					value={asString((params as any)?.file_format, 'txt')}
					placeholder="txt/csv/json/..."
					onInput={(event) => draft({ file_format: (event.currentTarget as HTMLInputElement).value })}

				/>
			</Field>
			<Field label="output mode">
				<ThemedSelect
					value={asString((params as any)?.output?.mode, 'text')}
					options={outputModeOptions}
					ariaLabel="guided object store output mode"
					onValueChange={(next) => {
						const mode = String(next) as SourceOutputMode;
						draft({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
						commit({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
					}}
				/>
			</Field>
		{:else}
			<Field label="provider">
				<ThemedSelect
					value={asString((params as any)?.provider, 'snowflake')}
					options={warehouseProviderOptions}
					ariaLabel="guided warehouse provider"
					onValueChange={(provider) => {
						draft({ provider });
						commit({ provider });
					}}
				/>
			</Field>
			<Field label="connection_ref">
				<Input
					value={asString((params as any)?.connection_ref, '')}
					placeholder="conn:warehouse_default"
					onInput={(event) => draft({ connection_ref: (event.currentTarget as HTMLInputElement).value })}

				/>
			</Field>
			<Field label="query">
				<Input
					multiline={true}
					rows={4}
					value={asString((params as any)?.query, '')}
					placeholder="select * from my_table"
					onInput={(event) => draft({ query: (event.currentTarget as HTMLTextAreaElement).value })}

				/>
			</Field>
			<Field label="limit">
				<Input
					type="number"
					min="1"
					step="1"
					value={asNumberOrEmpty((params as any)?.limit)}
					onInput={(event) => draft({ limit: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}

				/>
			</Field>
			<Field label="output mode">
				<ThemedSelect
					value={asString((params as any)?.output?.mode, 'table')}
					options={outputModeOptions}
					ariaLabel="guided warehouse output mode"
					onValueChange={(next) => {
						const mode = String(next) as SourceOutputMode;
						draft({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
						commit({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
					}}
				/>
			</Field>
		{/if}
	</Section>
{/if}
