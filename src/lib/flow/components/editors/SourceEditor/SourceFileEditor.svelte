<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceFileParams } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { getSnapshotMeta, uploadSnapshot } from '$lib/flow/client/runs';
	import { asBoolean, asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	type FileFormat = SourceFileParams['file_format'];
	type SourceFilePatch = Partial<SourceFileParams>;
	type RecentSnapshot = {
		id: string;
		filename?: string;
		importedAt?: string;
		size?: number;
		mimeType?: string;
	};

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let params: Partial<SourceFileParams>;
	export let onDraft: (patch: SourceFilePatch) => void;
	export let onCommit: (patch: SourceFilePatch) => void;

	const fileFormatOptions: FileFormat[] = ['csv', 'tsv', 'parquet', 'json', 'excel', 'txt', 'pdf'];
	const RECENT_LIMIT = 10;

	let fileInputEl: HTMLInputElement | null = null;
	let isUploading = false;
	let uploadError = '';
	let isDragOver = false;
	let loadingIds: string[] = [];
	let hydrationSignature = '';

	$: snapshotId = asString(params?.snapshotId, '').toLowerCase();
	$: recentSnapshots = normalizeRecentSnapshots((params as any)?.recentSnapshots, (params as any)?.recentSnapshotIds);
	$: snapshotMetadata = (params?.snapshotMetadata as Record<string, unknown> | undefined) ?? undefined;
	$: currentSnapshot = recentSnapshots.find((s) => s.id === snapshotId);
	$: file_format = (asString(params?.file_format, 'csv') as FileFormat) ?? 'csv';
	$: delimiter = asString(params?.delimiter, file_format === 'tsv' ? '\t' : ',');
	$: sheet_name = asString(params?.sheet_name, '');
	$: sample_size = asNumberOrEmpty(params?.sample_size);
	$: encoding = asString(params?.encoding, 'utf-8');
	$: cache_enabled = asBoolean(params?.cache_enabled, true);
	$: void hydrateMissingRecentSnapshots(recentSnapshots);

	function draft(patch: SourceFilePatch): void {
		onDraft?.(patch);
	}

	function commit(patch: SourceFilePatch): void {
		onCommit?.(patch);
	}

	function bytesLabel(size: unknown): string {
		const n = Number(size);
		if (!Number.isFinite(n) || n < 0) return '-';
		if (n < 1024) return `${n} B`;
		if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
		return `${(n / (1024 * 1024)).toFixed(1)} MB`;
	}

	function shortHash(hash: string): string {
		const h = asString(hash, '');
		return h ? `${h.slice(0, 8)}...` : '-';
	}

	function isSnapshotId(value: unknown): value is string {
		const id = asString(value, '').toLowerCase();
		return /^[a-f0-9]{64}$/.test(id);
	}

	function normalizeRecentSnapshots(rawEntries: unknown, rawIds: unknown): RecentSnapshot[] {
		const out: RecentSnapshot[] = [];
		const seen = new Set<string>();
		if (Array.isArray(rawEntries)) {
			for (const item of rawEntries) {
				if (!item || typeof item !== 'object') continue;
				const id = asString((item as any).id, '').toLowerCase();
				if (!isSnapshotId(id) || seen.has(id)) continue;
				seen.add(id);
				out.push({
					id,
					filename: asString((item as any).filename, '') || undefined,
					importedAt: asString((item as any).importedAt, '') || undefined,
					size: Number.isFinite(Number((item as any).size)) ? Number((item as any).size) : undefined,
					mimeType: asString((item as any).mimeType, '') || undefined
				});
			}
		}
		if (Array.isArray(rawIds)) {
			for (const idRaw of rawIds) {
				const id = asString(idRaw, '').toLowerCase();
				if (!isSnapshotId(id) || seen.has(id)) continue;
				seen.add(id);
				out.push({ id });
			}
		}
		return out.slice(0, RECENT_LIMIT);
	}

	function withRecentPatch(entries: RecentSnapshot[]): SourceFilePatch {
		const normalized = normalizeRecentSnapshots(entries, []);
		return {
			recentSnapshots: normalized,
			recentSnapshotIds: normalized.map((e) => e.id)
		};
	}

	function nextRecentEntries(incoming: RecentSnapshot, current: RecentSnapshot[]): RecentSnapshot[] {
		const id = asString(incoming.id, '').toLowerCase();
		if (!isSnapshotId(id)) return current;
		const merged: RecentSnapshot = {
			id,
			filename: incoming.filename,
			importedAt: incoming.importedAt,
			size: incoming.size,
			mimeType: incoming.mimeType
		};
		const rest = current.filter((s) => s.id !== id);
		return [merged, ...rest].slice(0, RECENT_LIMIT);
	}

	function optionLabel(entry: RecentSnapshot): string {
		const prefix = shortHash(entry.id);
		if (entry.filename) return `${entry.filename} ( ${prefix})`;
		return loadingIds.includes(entry.id) ? `${prefix} (loading...)` : prefix;
	}

	async function hydrateMissingRecentSnapshots(entries: RecentSnapshot[]): Promise<void> {
		const missing = entries.filter((e) => !e.filename).map((e) => e.id);
		const signature = missing.join(',');
		if (!signature) {
			hydrationSignature = '';
			return;
		}
		if (signature === hydrationSignature) return;
		hydrationSignature = signature;
		loadingIds = missing;

		const fetched = await Promise.all(
			missing.map(async (id) => {
				try {
					const res = await getSnapshotMeta(id);
					return {
						id,
						filename: asString(res?.metadata?.originalFilename, '') || undefined,
						importedAt: asString(res?.metadata?.importedAt, '') || undefined,
						size: Number.isFinite(Number(res?.metadata?.byteSize))
							? Number(res?.metadata?.byteSize)
							: undefined,
						mimeType: asString(res?.metadata?.mimeType, '') || undefined
					};
				} catch {
					return { id };
				}
			})
		);

		loadingIds = [];
		const map = new Map(fetched.map((f) => [f.id, f]));
		const updated = entries.map((entry) => {
			const incoming = map.get(entry.id);
			if (!incoming) return entry;
			return {
				...entry,
				filename: entry.filename ?? incoming.filename,
				importedAt: entry.importedAt ?? incoming.importedAt,
				size: entry.size ?? incoming.size,
				mimeType: entry.mimeType ?? incoming.mimeType
			};
		});
		if (JSON.stringify(updated) !== JSON.stringify(entries)) {
			const patch = withRecentPatch(updated);
			draft(patch);
			commit(patch);
		}
	}

	async function handleDroppedFile(file: File): Promise<void> {
		isUploading = true;
		uploadError = '';
		try {
			const result = await uploadSnapshot(file);
			const incoming: RecentSnapshot = {
				id: asString(result.snapshotId, '').toLowerCase(),
				filename: asString(result.metadata?.originalFilename, '') || undefined,
				importedAt: asString(result.metadata?.importedAt, '') || undefined,
				size: Number.isFinite(Number(result.metadata?.byteSize))
					? Number(result.metadata?.byteSize)
					: undefined,
				mimeType: asString(result.metadata?.mimeType, '') || undefined
			};
			const nextRecent = nextRecentEntries(incoming, recentSnapshots);
			const patch: SourceFilePatch = {
				snapshotId: incoming.id,
				snapshotMetadata: result.metadata,
				...withRecentPatch(nextRecent)
			};
			draft(patch);
			commit(patch);
		} catch (err) {
			uploadError = err instanceof Error ? err.message : String(err);
		} finally {
			isUploading = false;
		}
	}

	async function onFileInputChange(event: Event): Promise<void> {
		const input = event.currentTarget as HTMLInputElement;
		const file = input.files?.[0];
		if (!file) return;
		await handleDroppedFile(file);
		input.value = '';
	}

	async function onDrop(event: DragEvent): Promise<void> {
		event.preventDefault();
		event.stopPropagation();
		isDragOver = false;
		const file = event.dataTransfer?.files?.[0];
		if (!file) return;
		await handleDroppedFile(file);
	}

	function onDragOver(event: DragEvent): void {
		event.preventDefault();
		isDragOver = true;
	}

	function onDragLeave(event: DragEvent): void {
		event.preventDefault();
		isDragOver = false;
	}

	function onSelectPrevious(event: Event): void {
		const value = (event.currentTarget as HTMLSelectElement).value;
		const sid = asString(value, '').trim().toLowerCase();
		if (!isSnapshotId(sid)) return;
		const selected = recentSnapshots.find((s) => s.id === sid);
		const nextRecent = nextRecentEntries(selected ?? { id: sid }, recentSnapshots);
		const patch: SourceFilePatch = {
			snapshotId: sid,
			snapshotMetadata: {
				snapshotId: sid,
				originalFilename: selected?.filename,
				importedAt: selected?.importedAt,
				byteSize: selected?.size,
				mimeType: selected?.mimeType
			},
			...withRecentPatch(nextRecent)
		};
		draft(patch);
		commit(patch);
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
</script>

{#if selectedNode}
	<Section title="File">
		<Field>
			<div
				class={`dropzone ${isDragOver ? 'dragOver' : ''}`}
				role="button"
				tabindex="0"
				on:drop={onDrop}
				on:dragover={onDragOver}
				on:dragleave={onDragLeave}
				on:click={() => fileInputEl?.click()}
				on:keydown={(event) => {
					if (event.key === 'Enter' || event.key === ' ') {
						event.preventDefault();
						fileInputEl?.click();
					}
				}}
			>
				<div class="dropTitle">{isUploading ? 'Uploading...' : 'Drop file here'}</div>
				<button type="button" class="small" disabled={isUploading} on:click={() => fileInputEl?.click()}>
					Choose File
				</button>
				<input bind:this={fileInputEl} type="file" hidden on:change={onFileInputChange} />
			</div>
			{#if uploadError}
				<div class="error">{uploadError}</div>
			{/if}
		</Field>

		<Field label="current snapshot">
			<div class="snapshotMeta">
				<div><b>id</b> {snapshotId ? shortHash(snapshotId) : '-'}</div>
				<div><b>file</b> {asString(snapshotMetadata?.originalFilename, currentSnapshot?.filename ?? '-')}</div>
				<div><b>size</b> {bytesLabel(snapshotMetadata?.byteSize ?? currentSnapshot?.size)}</div>
				<div><b>imported</b> {asString(snapshotMetadata?.importedAt, currentSnapshot?.importedAt ?? '-')}</div>
			</div>
		</Field>

		<Field label="previous uploads">
			<select value="" on:change={onSelectPrevious}>
				<option value="" disabled selected>Previous uploads (snapshots)</option>
				{#each recentSnapshots as entry}
					<option value={entry.id}>{optionLabel(entry)}</option>
				{/each}
			</select>
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
	.dropzone {
		border: 1px dashed rgba(148, 163, 184, 0.7);
		border-radius: 10px;
		padding: 10px;
		display: grid;
		gap: 6px;
	}
	.dropzone.dragOver {
		border-color: rgba(56, 189, 248, 0.9);
		background: rgba(15, 23, 42, 0.35);
	}
	.dropTitle {
		font-weight: 600;
	}
	.error {
		margin-top: 6px;
		font-size: 12px;
		color: #f87171;
	}
	.snapshotMeta {
		display: grid;
		gap: 4px;
		font-size: 12px;
	}
</style>
