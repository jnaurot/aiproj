<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceFileParams } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { getSnapshotMeta, uploadSnapshot } from '$lib/flow/client/runs';
	import { asBoolean, asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';
	import {
		type RecentSnapshot,
		mergeRecentSnapshotOnUpload,
		normalizeRecentSnapshots,
		optionLabel as recentOptionLabel,
		sortRecentSnapshotsForDisplay,
		updateRecentSnapshotInPlace
	} from './sourceFileSnapshots';

	type FileFormat = SourceFileParams['file_format'];
	type SourceFilePatch = Partial<SourceFileParams>;
	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let params: Partial<SourceFileParams>;
	export let onDraft: (patch: SourceFilePatch) => void;
	export let onCommit: (patch: SourceFilePatch) => void;
	export let onSnapshotCommit: ((patch: SourceFilePatch) => void | Promise<unknown>) | undefined = undefined;

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
	$: displayRecentSnapshots = sortRecentSnapshotsForDisplay(recentSnapshots);
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

	function commitSnapshot(patch: SourceFilePatch): void {
		if (onSnapshotCommit) {
			void onSnapshotCommit(patch);
			return;
		}
		commit(patch);
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

	function withRecentPatch(entries: RecentSnapshot[]): SourceFilePatch {
		const normalized = normalizeRecentSnapshots(entries, []);
		return {
			recentSnapshots: normalized,
			recentSnapshotIds: normalized.map((e) => e.id)
		};
	}

	function optionLabel(entry: RecentSnapshot): string {
		return recentOptionLabel(entry, loadingIds.includes(entry.id), shortHash);
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
		let updated = entries;
		for (const [id, incoming] of map.entries()) {
			updated = updateRecentSnapshotInPlace(updated, id, incoming);
		}
		if (JSON.stringify(updated) !== JSON.stringify(entries)) {
			const patch = withRecentPatch(updated);
			commitSnapshot(patch);
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
			const nextRecent = mergeRecentSnapshotOnUpload(incoming, recentSnapshots, RECENT_LIMIT);
			const patch: SourceFilePatch = {
				snapshotId: incoming.id,
				snapshotMetadata: result.metadata,
				...withRecentPatch(nextRecent)
			};
			commitSnapshot(patch);
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

	async function onSelectPrevious(event: Event): Promise<void> {
		const value = (event.currentTarget as HTMLSelectElement).value;
		const sid = asString(value, '').trim().toLowerCase();
		if (!/^[a-f0-9]{64}$/.test(sid)) return;
		const selected = recentSnapshots.find((s) => s.id === sid);
		let nextRecent = recentSnapshots;
		let resolved = selected;
		if (!selected?.filename) {
			try {
				const meta = await getSnapshotMeta(sid);
				const incoming: RecentSnapshot = {
					id: sid,
					filename: asString(meta?.metadata?.originalFilename, '') || undefined,
					importedAt: asString(meta?.metadata?.importedAt, '') || undefined,
					size: Number.isFinite(Number(meta?.metadata?.byteSize))
						? Number(meta?.metadata?.byteSize)
						: undefined,
					mimeType: asString(meta?.metadata?.mimeType, '') || undefined
				};
				nextRecent = updateRecentSnapshotInPlace(recentSnapshots, sid, incoming);
				resolved = nextRecent.find((s) => s.id === sid) ?? selected;
			} catch {
				resolved = selected;
			}
		}
		const patch: SourceFilePatch = {
			snapshotId: sid,
			snapshotMetadata: {
				snapshotId: sid,
				originalFilename: resolved?.filename,
				importedAt: resolved?.importedAt,
				byteSize: resolved?.size,
				mimeType: resolved?.mimeType
			},
			...withRecentPatch(nextRecent)
		};
		commitSnapshot(patch);
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
				{#each displayRecentSnapshots as entry}
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
