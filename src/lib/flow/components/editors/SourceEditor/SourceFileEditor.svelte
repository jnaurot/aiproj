<script lang="ts">
	// lib/flow/components/editors/SourceEditor/SourceFileEditor.svelte
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import BoolSelect from '$lib/flow/components/BoolSelect.svelte';
	import NumberInput from '$lib/flow/components/NumberInput.svelte';

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;

	export let params: any;
	export let onDraft: (patch: Record<string, any>) => void;
	export let onCommit: (patch: Record<string, any>) => void;

	let fileEl: HTMLInputElement | null = null;

	function draft(patch: Record<string, any>) {
		onDraft?.(patch);
	}

	function commit(patch: Record<string, any>) {
		onCommit?.(patch);
	}

	function setFileFormat(ff: string) {
		const next: Record<string, any> = { file_format: ff };

		if (ff === 'csv' || ff === 'tsv') {
			next.delimiter = params.delimiter ?? (ff === 'tsv' ? '\t' : ',');
			next.sheet_name = undefined;
		} else if (ff === 'excel') {
			next.sheet_name = params.sheet_name ?? '';
			next.delimiter = undefined;
		} else {
			next.delimiter = undefined;
			next.sheet_name = undefined;
		}

		commit(next);
	}
</script>

{#if selectedNode}
	<div class="section">
		<div class="sectionTitle">File</div>

		<div class="group">
			<!-- 1) File chooser row -->
			<div class="field">
				<div class="k">file</div>
				<div class="v">
					<!-- hidden native input -->
					<input
						bind:this={fileEl}
						type="file"
						accept=".csv,.tsv,.xlsx,.json,.txt,.parquet,.pdf"
						style="display:none"
						on:change={(e) => {
							const input = e.currentTarget as HTMLInputElement;
							const f = input.files?.[0];
							if (!f) return;

							commit({
								file_path: `localfile://${encodeURIComponent(f.name)}`,
								file_name: f.name,
								file_size: f.size,
								file_mime: f.type
							});
						}}
					/>

					<!-- visible, controlled UI -->
					<div style="display:flex; align-items:center; gap:10px;">
						<button type="button" on:click={() => fileEl?.click()}> Choose file </button>
						<span style="opacity:.8;">
							{params.file_name ?? 'No file selected'}
						</span>
					</div>
				</div>
			</div>

			<!-- 2) File path row -->
			<div class="field">
				<div class="k">file path</div>
				<div class="v">
					<input
						value={params.file_path ?? ''}
						placeholder="C:/path/to/file.csv"
						on:input={(e) => draft({ file_path: (e.currentTarget as HTMLInputElement).value })}
					/>
				</div>
			</div>

			<!-- 3) File format row (dropdown = immediate commit) -->
			<div class="field">
				<div class="k">file_format</div>
				<div class="v">
					<select
						value={params.file_format ?? 'csv'}
						on:change={(e) => setFileFormat((e.currentTarget as HTMLSelectElement).value)}
					>
						<option value="csv">csv</option>
						<option value="tsv">tsv</option>
						<option value="parquet">parquet</option>
						<option value="json">json</option>
						<option value="excel">excel</option>
						<option value="txt">txt</option>
						<option value="pdf">pdf</option>
					</select>
				</div>
			</div>

			{#if (params.file_format ?? 'csv') === 'csv' || (params.file_format ?? 'csv') === 'tsv'}
				<div class="field">
					<div class="k">delimiter</div>
					<div class="v">
						<input
							value={params.delimiter ?? ((params.file_format ?? 'csv') === 'tsv' ? '\t' : ',')}
							placeholder={(params.file_format ?? 'csv') === 'tsv' ? '\\t' : ','}
							on:input={(e) => draft({ delimiter: (e.currentTarget as HTMLInputElement).value })}
						/>
					</div>
				</div>
			{/if}

			{#if (params.file_format ?? 'csv') === 'excel'}
				<div class="field">
					<div class="k">sheet_name</div>
					<div class="v">
						<input
							value={params.sheet_name ?? ''}
							placeholder="Sheet1 (blank = first sheet)"
							on:input={(e) => {
								const v = (e.currentTarget as HTMLInputElement).value.trim();
								draft({ sheet_name: v === '' ? undefined : v });
							}}
						/>
					</div>
				</div>
			{/if}

			<div class="field">
				<div class="k">sample_size</div>
				<div class="v">
					<NumberInput
						label=""
						value={params.sample_size}
						placeholder="e.g. 1000"
						min={1}
						onChange={(v) => draft({ sample_size: v })}
					/>
				</div>
			</div>

			<div class="field">
				<div class="k">encoding</div>
				<div class="v">
					<input
						value={params.encoding ?? 'utf-8'}
						placeholder="utf-8"
						on:input={(e) => draft({ encoding: (e.currentTarget as HTMLInputElement).value })}
					/>
				</div>
			</div>

			<div class="field">
				<div class="k">cache_enabled</div>
				<div class="v">
					<BoolSelect
						label=""
						value={params.cache_enabled ?? true}
						onChange={(v) => draft({ cache_enabled: v })}
					/>
				</div>
			</div>
		</div>
	</div>
{/if}

<style>
	.section {
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 12px;
		padding: 12px;
		background: rgba(255, 255, 255, 0.03);
	}

	.sectionTitle {
		font-weight: 650;
		font-size: 14px;
		margin-bottom: 10px;
		opacity: 0.9;
	}

	.group {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}

	.field {
		display: grid;
		grid-template-columns: 100px minmax(0, 1fr);
		align-items: start;
		gap: 8px;
	}

	.k {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
	}

	.v {
		min-width: 0;
	}

	input,
	select {
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

	input:focus,
	select:focus {
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
