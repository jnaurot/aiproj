<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceFileParams } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { asBoolean, asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	type FileFormat = SourceFileParams['file_format'];
	type SourceFilePatch = Partial<SourceFileParams>;

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let params: Partial<SourceFileParams>;
	export let onDraft: (patch: SourceFilePatch) => void;
	export let onCommit: (patch: SourceFilePatch) => void;

	let fileEl: HTMLInputElement | null = null;

	const fileFormatOptions: FileFormat[] = ['csv', 'tsv', 'parquet', 'json', 'excel', 'txt', 'pdf'];

	$: file_path = asString(params?.file_path, '');
	$: file_name = asString(params?.file_name, 'No file selected');
	$: file_format = (asString(params?.file_format, 'csv') as FileFormat) ?? 'csv';
	$: delimiter = asString(params?.delimiter, file_format === 'tsv' ? '\t' : ',');
	$: sheet_name = asString(params?.sheet_name, '');
	$: sample_size = asNumberOrEmpty(params?.sample_size);
	$: encoding = asString(params?.encoding, 'utf-8');
	$: cache_enabled = asBoolean(params?.cache_enabled, true);

	function draft(patch: SourceFilePatch): void {
		onDraft?.(patch);
	}

	function commit(patch: SourceFilePatch): void {
		onCommit?.(patch);
	}

	function setFileFormat(nextFormat: string): void {
		if (!fileFormatOptions.includes(nextFormat as FileFormat)) return;
		const next = nextFormat as FileFormat;
		const patch: SourceFilePatch = { file_format: next };

		if (next === 'csv' || next === 'tsv') {
			patch.delimiter = params?.delimiter ?? (next === 'tsv' ? '\t' : ',');
			patch.sheet_name = undefined;
		} else if (next === 'excel') {
			patch.sheet_name = params?.sheet_name ?? '';
			patch.delimiter = undefined;
		} else {
			patch.delimiter = undefined;
			patch.sheet_name = undefined;
		}

		commit(patch);
	}

	function onFilePicked(event: Event): void {
		const input = event.currentTarget as HTMLInputElement | null;
		const file = input?.files?.[0];
		if (!file) return;

		commit({
			file_path: `localfile://${encodeURIComponent(file.name)}`,
			file_name: file.name,
			file_size: file.size,
			file_mime: file.type
		});
	}
</script>

{#if selectedNode}
	<Section title="File">
		<Field label="file">
			<div class="fileRow">
				<input
					bind:this={fileEl}
					type="file"
					accept=".csv,.tsv,.xlsx,.json,.txt,.parquet,.pdf"
					style="display:none"
					on:change={onFilePicked}
				/>
				<button type="button" on:click={() => fileEl?.click()}>Choose file</button>
				<span class="fileName">{file_name}</span>
			</div>
		</Field>

		<Field label="file path">
			<Input
				value={file_path}
				placeholder="C:/path/to/file.csv"
				onInput={(event) => draft({ file_path: (event.currentTarget as HTMLInputElement).value })}
				onBlur={(event) => commit({ file_path: (event.currentTarget as HTMLInputElement).value })}
			/>
		</Field>

		<Field label="file_format">
			<select
				value={file_format}
				on:change={(event) => setFileFormat((event.currentTarget as HTMLSelectElement).value)}
			>
				{#each fileFormatOptions as option}
					<option value={option}>{option}</option>
				{/each}
			</select>
		</Field>

		{#if file_format === 'csv' || file_format === 'tsv'}
			<Field label="delimiter">
				<Input
					value={delimiter}
					placeholder={file_format === 'tsv' ? '\\t' : ','}
					onInput={(event) => draft({ delimiter: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ delimiter: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>
		{/if}

		{#if file_format === 'excel'}
			<Field label="sheet_name">
				<Input
					value={sheet_name}
					placeholder="Sheet1 (blank = first sheet)"
					onInput={(event) => {
						const value = (event.currentTarget as HTMLInputElement).value;
						draft({ sheet_name: value.trim() === '' ? undefined : value });
					}}
					onBlur={(event) => {
						const value = (event.currentTarget as HTMLInputElement).value;
						commit({ sheet_name: value.trim() === '' ? undefined : value });
					}}
				/>
			</Field>
		{/if}

		<Field label="sample_size">
			<Input
				type="number"
				min="1"
				step="1"
				value={sample_size}
				placeholder="e.g. 1000"
				onInput={(event) =>
					draft({ sample_size: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
				onBlur={(event) =>
					commit({ sample_size: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
			/>
		</Field>

		<Field label="encoding">
			<Input
				value={encoding}
				placeholder="utf-8"
				onInput={(event) => draft({ encoding: (event.currentTarget as HTMLInputElement).value })}
				onBlur={(event) => commit({ encoding: (event.currentTarget as HTMLInputElement).value })}
			/>
		</Field>

		<Field label="cache_enabled">
			<Input
				type="checkbox"
				checked={cache_enabled}
				onChange={(event) => {
					const checked = (event.currentTarget as HTMLInputElement).checked;
					draft({ cache_enabled: checked });
					commit({ cache_enabled: checked });
				}}
			/>
		</Field>
	</Section>
{/if}

<style>
	.fileRow {
		display: flex;
		align-items: center;
		gap: 10px;
	}

	.fileName {
		opacity: 0.8;
	}
</style>
