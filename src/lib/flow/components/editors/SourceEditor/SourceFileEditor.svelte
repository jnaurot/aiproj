<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceFileParams } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type FileFormat = SourceFileParams['file_format'];
	type EditorParams = Partial<SourceFileParams> & {
		file_name?: string;
		file_size?: number;
		file_mime?: string;
	};

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let params: EditorParams;
	export let onDraft: (patch: Record<string, unknown>) => void;
	export let onCommit: (patch: Record<string, unknown>) => void;

	let fileEl: HTMLInputElement | null = null;

	const fileFormatOptions: FileFormat[] = ['csv', 'tsv', 'parquet', 'json', 'excel', 'txt', 'pdf'];

	const asString = (value: unknown, fallback = ''): string =>
		typeof value === 'string' ? value : fallback;

	const asBoolean = (value: unknown, fallback: boolean): boolean =>
		typeof value === 'boolean' ? value : fallback;

	const asNumberOrEmpty = (value: unknown): string =>
		typeof value === 'number' && Number.isFinite(value) ? String(value) : '';

	$: file_path = asString(params?.file_path, '');
	$: file_format = (asString(params?.file_format, 'csv') as FileFormat) ?? 'csv';
	$: delimiter = asString(params?.delimiter, file_format === 'tsv' ? '\t' : ',');
	$: sheet_name = asString(params?.sheet_name, '');
	$: sample_size = asNumberOrEmpty(params?.sample_size);
	$: encoding = asString(params?.encoding, 'utf-8');
	$: cache_enabled = asBoolean(params?.cache_enabled, true);
	$: selected_file_name = asString(params?.file_name, 'No file selected');

	function draft(patch: Record<string, unknown>): void {
		onDraft?.(patch);
	}

	function commit(patch: Record<string, unknown>): void {
		onCommit?.(patch);
	}

	function parsePositiveInt(raw: string): number | undefined {
		if (raw.trim() === '') return undefined;
		const value = Number.parseInt(raw, 10);
		return Number.isFinite(value) && value > 0 ? value : undefined;
	}

	function setFileFormat(nextFormat: string): void {
		if (!fileFormatOptions.includes(nextFormat as FileFormat)) return;
		const ff = nextFormat as FileFormat;
		const next: Record<string, unknown> = { file_format: ff };

		if (ff === 'csv' || ff === 'tsv') {
			next.delimiter = params?.delimiter ?? (ff === 'tsv' ? '\t' : ',');
			next.sheet_name = undefined;
		} else if (ff === 'excel') {
			next.sheet_name = params?.sheet_name ?? '';
			next.delimiter = undefined;
		} else {
			next.delimiter = undefined;
			next.sheet_name = undefined;
		}

		commit(next);
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
				<span class="fileName">{selected_file_name}</span>
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
			<select value={file_format} on:change={(event) => setFileFormat((event.currentTarget as HTMLSelectElement).value)}>
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
			<input
				type="number"
				min="1"
				step="1"
				value={sample_size}
				placeholder="e.g. 1000"
				on:input={(event) => draft({ sample_size: parsePositiveInt((event.currentTarget as HTMLInputElement).value) })}
				on:blur={(event) => commit({ sample_size: parsePositiveInt((event.currentTarget as HTMLInputElement).value) })}
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
			<input
				type="checkbox"
				checked={cache_enabled}
				on:change={(event) => {
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

	select,
	input[type='number'] {
		width: 100%;
		box-sizing: border-box;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(0, 0, 0, 0.2);
		color: inherit;
		padding: 8px 10px;
		font-size: 14px;
		outline: none;
		min-height: 40px;
	}

	select:focus,
	input[type='number']:focus {
		border-color: rgba(255, 255, 255, 0.25);
	}

	button {
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.06);
		color: inherit;
		padding: 6px 10px;
		border-radius: 10px;
		cursor: pointer;
		font-size: 13px;
	}
</style>
