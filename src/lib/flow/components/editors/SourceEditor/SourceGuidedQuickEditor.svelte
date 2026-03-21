<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceKind } from '$lib/flow/types/paramsMap';
	import type { SourceOutputMode } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let sourceKind: SourceKind = 'file';
	export let params: Record<string, unknown> = {};
	export let onDraft: (patch: Record<string, unknown>) => void;
	export let onCommit: (patch: Record<string, unknown>) => void;

	const outputModes: SourceOutputMode[] = ['table', 'text', 'json', 'binary'];

	function draft(patch: Record<string, unknown>): void {
		onDraft?.(patch);
	}

	function commit(patch: Record<string, unknown>): void {
		onCommit?.(patch);
	}
</script>

{#if selectedNode}
	<Section title="Guided Source Setup">
		{#if sourceKind === 'file'}
			<Field label="filename">
				<Input
					value={asString((params as any)?.filename, '')}
					placeholder="data.txt"
					onInput={(event) => draft({ filename: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ filename: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>
			<Field label="file_format">
				<Input
					value={asString((params as any)?.file_format, 'txt')}
					placeholder="txt/csv/json/..."
					onInput={(event) => draft({ file_format: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ file_format: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>
			<Field label="output mode">
				<select
					value={asString((params as any)?.output?.mode, 'text')}
					on:change={(event) => {
						const mode = (event.currentTarget as HTMLSelectElement).value as SourceOutputMode;
						draft({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
						commit({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
					}}
				>
					{#each outputModes as mode}
						<option value={mode}>{mode}</option>
					{/each}
				</select>
			</Field>
			<Field label="cache_enabled">
				<select
					value={String(Boolean((params as any)?.cache_enabled ?? true))}
					on:change={(event) => {
						const enabled = (event.currentTarget as HTMLSelectElement).value === 'true';
						draft({ cache_enabled: enabled });
						commit({ cache_enabled: enabled });
					}}
				>
					<option value="true">true</option>
					<option value="false">false</option>
				</select>
			</Field>
		{:else if sourceKind === 'database'}
			<Field label="connection_ref">
				<Input
					value={asString((params as any)?.connection_ref, '')}
					placeholder="conn:default"
					onInput={(event) => draft({ connection_ref: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ connection_ref: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>
			<Field label="query">
				<Input
					multiline={true}
					rows={4}
					value={asString((params as any)?.query, '')}
					placeholder="select * from my_table"
					onInput={(event) => draft({ query: (event.currentTarget as HTMLTextAreaElement).value })}
					onBlur={(event) => commit({ query: (event.currentTarget as HTMLTextAreaElement).value })}
				/>
			</Field>
			<Field label="limit">
				<Input
					type="number"
					min="1"
					step="1"
					value={asNumberOrEmpty((params as any)?.limit)}
					onInput={(event) => draft({ limit: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
					onBlur={(event) => commit({ limit: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
				/>
			</Field>
			<Field label="output mode">
				<select
					value={asString((params as any)?.output?.mode, 'table')}
					on:change={(event) => {
						const mode = (event.currentTarget as HTMLSelectElement).value as SourceOutputMode;
						draft({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
						commit({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
					}}
				>
					{#each outputModes as mode}
						<option value={mode}>{mode}</option>
					{/each}
				</select>
			</Field>
		{:else if sourceKind === 'api'}
			<Field label="method">
				<select
					value={asString((params as any)?.method, 'GET')}
					on:change={(event) => {
						const method = (event.currentTarget as HTMLSelectElement).value;
						draft({ method });
						commit({ method });
					}}
				>
					<option value="GET">GET</option>
					<option value="POST">POST</option>
					<option value="PUT">PUT</option>
					<option value="PATCH">PATCH</option>
					<option value="DELETE">DELETE</option>
					<option value="HEAD">HEAD</option>
				</select>
			</Field>
			<Field label="url">
				<Input
					value={asString((params as any)?.url, '')}
					placeholder="https://api.example.com/data"
					onInput={(event) => draft({ url: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ url: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>
			<Field label="auth_type">
				<select
					value={asString((params as any)?.auth_type, 'none')}
					on:change={(event) => {
						const auth_type = (event.currentTarget as HTMLSelectElement).value;
						draft({ auth_type });
						commit({ auth_type });
					}}
				>
					<option value="none">none</option>
					<option value="bearer">bearer</option>
					<option value="basic">basic</option>
					<option value="api_key">api_key</option>
				</select>
			</Field>
			<Field label="auth_token_ref">
				<Input
					value={asString((params as any)?.auth_token_ref, '')}
					placeholder="API_TOKEN"
					onInput={(event) => draft({ auth_token_ref: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ auth_token_ref: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>
			<Field label="output mode">
				<select
					value={asString((params as any)?.output?.mode, 'json')}
					on:change={(event) => {
						const mode = (event.currentTarget as HTMLSelectElement).value as SourceOutputMode;
						draft({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
						commit({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
					}}
				>
					{#each outputModes as mode}
						<option value={mode}>{mode}</option>
					{/each}
				</select>
			</Field>
		{:else if sourceKind === 'object_store'}
			<Field label="provider">
				<select
					value={asString((params as any)?.provider, 's3')}
					on:change={(event) => {
						const provider = (event.currentTarget as HTMLSelectElement).value;
						draft({ provider });
						commit({ provider });
					}}
				>
					<option value="s3">s3</option>
					<option value="azure_blob">azure_blob</option>
					<option value="gcs">gcs</option>
				</select>
			</Field>
			<Field label="bucket">
				<Input
					value={asString((params as any)?.bucket, '')}
					placeholder="my-bucket"
					onInput={(event) => draft({ bucket: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ bucket: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>
			<Field label="key">
				<Input
					value={asString((params as any)?.key, '')}
					placeholder="path/to/object"
					onInput={(event) => draft({ key: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ key: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>
			<Field label="file_format">
				<Input
					value={asString((params as any)?.file_format, 'txt')}
					placeholder="txt/csv/json/..."
					onInput={(event) => draft({ file_format: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ file_format: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>
			<Field label="output mode">
				<select
					value={asString((params as any)?.output?.mode, 'text')}
					on:change={(event) => {
						const mode = (event.currentTarget as HTMLSelectElement).value as SourceOutputMode;
						draft({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
						commit({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
					}}
				>
					{#each outputModes as mode}
						<option value={mode}>{mode}</option>
					{/each}
				</select>
			</Field>
		{:else}
			<Field label="provider">
				<select
					value={asString((params as any)?.provider, 'snowflake')}
					on:change={(event) => {
						const provider = (event.currentTarget as HTMLSelectElement).value;
						draft({ provider });
						commit({ provider });
					}}
				>
					<option value="snowflake">snowflake</option>
					<option value="bigquery">bigquery</option>
					<option value="databricks_sql">databricks_sql</option>
				</select>
			</Field>
			<Field label="connection_ref">
				<Input
					value={asString((params as any)?.connection_ref, '')}
					placeholder="conn:warehouse_default"
					onInput={(event) => draft({ connection_ref: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ connection_ref: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>
			<Field label="query">
				<Input
					multiline={true}
					rows={4}
					value={asString((params as any)?.query, '')}
					placeholder="select * from my_table"
					onInput={(event) => draft({ query: (event.currentTarget as HTMLTextAreaElement).value })}
					onBlur={(event) => commit({ query: (event.currentTarget as HTMLTextAreaElement).value })}
				/>
			</Field>
			<Field label="limit">
				<Input
					type="number"
					min="1"
					step="1"
					value={asNumberOrEmpty((params as any)?.limit)}
					onInput={(event) => draft({ limit: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
					onBlur={(event) => commit({ limit: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
				/>
			</Field>
			<Field label="output mode">
				<select
					value={asString((params as any)?.output?.mode, 'table')}
					on:change={(event) => {
						const mode = (event.currentTarget as HTMLSelectElement).value as SourceOutputMode;
						draft({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
						commit({ output: { ...(((params as any)?.output as Record<string, unknown>) ?? {}), mode } });
					}}
				>
					{#each outputModes as mode}
						<option value={mode}>{mode}</option>
					{/each}
				</select>
			</Field>
		{/if}
	</Section>
{/if}
