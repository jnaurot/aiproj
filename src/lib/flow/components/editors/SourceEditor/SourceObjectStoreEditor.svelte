<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceObjectStoreParams, SourceOutputMode } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { asString } from '$lib/flow/components/editors/shared';

	type SourceObjectStorePatch = Partial<SourceObjectStoreParams>;
	type Provider = SourceObjectStoreParams['provider'];

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let params: Partial<SourceObjectStoreParams>;
	export let onDraft: (patch: SourceObjectStorePatch) => void;
	export let onCommit: (patch: SourceObjectStorePatch) => void;

	const providers: Provider[] = ['s3', 'azure_blob', 'gcs'];
	const outputModes: SourceOutputMode[] = ['table', 'text', 'json', 'binary'];

	$: provider = (asString(params?.provider, 's3') as Provider) ?? 's3';
	$: connection_ref = asString(params?.connection_ref, '');
	$: bucket = asString(params?.bucket, '');
	$: key = asString(params?.key, '');
	$: file_format = asString(params?.file_format, 'txt');
	$: encoding = asString(params?.encoding, 'utf-8');
	$: outputMode = (asString(params?.output?.mode, 'text') as SourceOutputMode) ?? 'text';

	function draft(patch: SourceObjectStorePatch): void {
		onDraft?.(patch);
	}

	function commit(patch: SourceObjectStorePatch): void {
		onCommit?.(patch);
	}
</script>

{#if selectedNode}
	<Section title="Object Store">
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

		<Field label="connection_ref">
			<Input
				value={connection_ref}
				placeholder="conn:object_store_default"
				onInput={(event) => draft({ connection_ref: (event.currentTarget as HTMLInputElement).value })}

			/>
		</Field>

		<Field label="bucket">
			<Input
				value={bucket}
				placeholder="my-bucket"
				onInput={(event) => draft({ bucket: (event.currentTarget as HTMLInputElement).value })}

			/>
		</Field>

		<Field label="key">
			<Input
				value={key}
				placeholder="path/to/file.csv"
				onInput={(event) => draft({ key: (event.currentTarget as HTMLInputElement).value })}

			/>
		</Field>

		<Field label="file_format">
			<Input
				value={file_format}
				placeholder="txt/csv/json/parquet/..."
				onInput={(event) => draft({ file_format: (event.currentTarget as HTMLInputElement).value as any })}

			/>
		</Field>

		<Field label="encoding">
			<Input
				value={encoding}
				placeholder="utf-8"
				onInput={(event) => draft({ encoding: (event.currentTarget as HTMLInputElement).value })}

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
