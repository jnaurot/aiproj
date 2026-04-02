<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceFileParams } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ThemedSelect, { type ThemedSelectOption } from '$lib/flow/components/ui/ThemedSelect.svelte';
	import SourceCapabilityBanner from './SourceCapabilityBanner.svelte';
	import SourceEffectivePreview from './SourceEffectivePreview.svelte';
	import { getSnapshotMeta, uploadSnapshot } from '$lib/flow/client/runs';
	import {
		asBoolean,
		asString
	} from '$lib/flow/components/editors/shared';
	import {
		effectiveConfigForSource,
		fileAutoAdjustmentNotices
	} from './sourceEffectiveConfig';
	import {
		type RecentSnapshot,
		mergeRecentSnapshotOnUpload,
		normalizeRecentSnapshots,
		optionLabel as recentOptionLabel,
		sortRecentSnapshotsForDisplay,
		updateRecentSnapshotInPlace
	} from './sourceFileSnapshots';
	import { emitSourceEditorTelemetry, makeAutoAdjustmentEvent } from './sourceEditorTelemetry';
	import { detectFileFormatFromFilename } from './fileFormatDetect';

	type FileFormat = SourceFileParams['file_format'];
	type SourceFilePatch = Partial<SourceFileParams>;
	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let params: Partial<SourceFileParams>;
	export let onDraft: (patch: SourceFilePatch) => void;
	export let onCommit: (patch: SourceFilePatch) => void;
	export let nodeError: Record<string, unknown> | null = null;
	export let onSnapshotCommit: ((patch: SourceFilePatch) => void | Promise<unknown>) | undefined =
		undefined;

	const baseFileFormatOptions: FileFormat[] = [
		'csv',
		'tsv',
		'parquet',
		'json',
		'excel',
		'txt',
		'pdf'
	];
	const imageFileFormatOptions: FileFormat[] = [
		'png',
		'jpg',
		'jpeg',
		'webp',
		'gif',
		'svg',
		'tiff',
		'tif'
	];
	const audioFileFormatOptions: FileFormat[] = [
		'mp3',
		'wav',
		'flac',
		'ogg',
		'm4a',
		'aac'
	];
	const videoFileFormatOptions: FileFormat[] = ['mp4', 'mov', 'webm'];
	const fileFormatOptions: FileFormat[] = [
		...baseFileFormatOptions,
		...imageFileFormatOptions,
		...audioFileFormatOptions,
		...videoFileFormatOptions
	];
	const fileFormatSelectOptions: ThemedSelectOption[] = [
		...baseFileFormatOptions.map((value) => ({ value, label: value })),
		...imageFileFormatOptions.map((value) => ({ value, label: `image - ${value}` })),
		...audioFileFormatOptions.map((value) => ({ value, label: `audio - ${value}` })),
		...videoFileFormatOptions.map((value) => ({ value, label: `video - ${value}` }))
	];
	const previousUploadOptions = (entries: RecentSnapshot[]): ThemedSelectOption[] => [
		{ value: '', label: 'Choose a previous upload...', disabled: true },
		...entries.map((entry) => ({
			value: entry.id,
			label: optionLabel(entry)
		}))
	];
	const hasHeaderOptions: ThemedSelectOption[] = [
		{ value: 'auto', label: 'auto' },
		{ value: 'yes', label: 'yes' },
		{ value: 'no', label: 'no' }
	];
	const malformedRowOptions: ThemedSelectOption[] = [
		{ value: 'fail', label: 'fail' },
		{ value: 'skip', label: 'skip' },
		{ value: 'warn', label: 'warn' }
	];
	const decimalSeparatorOptions: ThemedSelectOption[] = [
		{ value: '.', label: '.' },
		{ value: ',', label: ',' }
	];
	const jsonModeOptions: ThemedSelectOption[] = [
		{ value: 'auto', label: 'auto' },
		{ value: 'document', label: 'document' },
		{ value: 'ndjson', label: 'ndjson' }
	];
	const txtRecordModeOptions: ThemedSelectOption[] = [
		{ value: 'raw', label: 'raw' },
		{ value: 'lines', label: 'lines' },
		{ value: 'paragraphs', label: 'paragraphs' },
		{ value: 'fixed_chunk', label: 'fixed_chunk' }
	];
	const audioTranscodeOptions: ThemedSelectOption[] = [
		{ value: '', label: 'none' },
		...audioFileFormatOptions.map((value) => ({ value, label: value }))
	];
	const videoFrameModeOptions: ThemedSelectOption[] = [
		{ value: 'none', label: 'none' },
		{ value: 'keyframes', label: 'keyframes' },
		{ value: 'interval', label: 'interval' }
	];
	const encodingOptions: ThemedSelectOption[] = [
		{ value: 'utf-8', label: 'utf-8' },
		{ value: 'windows-1252', label: 'windows-1252' },
		{ value: 'iso-8859-1', label: 'iso-8859-1' },
		{ value: 'iso-8859-15', label: 'iso-8859-15' },
		{ value: 'utf-16le', label: 'utf-16le' },
		{ value: 'utf-16be', label: 'utf-16be' },
		{ value: 'us-ascii', label: 'us-ascii' }
	];
	const RECENT_LIMIT = 10;
	const ADJUSTMENT_LOG_LIMIT = 6;
	const recentAdjustmentsByNode = new Map<string, string[]>();

	let fileInputEl: HTMLInputElement | null = null;
	let isUploading = false;
	let uploadError = '';
	let isDragOver = false;
	let loadingIds: string[] = [];
	let hydrationSignature = '';
	let recentAdjustments: string[] = [];
	$: void nodeError;

	$: snapshotId = asString(params?.snapshotId, '').toLowerCase();
	$: recentSnapshots = normalizeRecentSnapshots(
		(params as any)?.recentSnapshots,
		(params as any)?.recentSnapshotIds
	);
	$: displayRecentSnapshots = sortRecentSnapshotsForDisplay(recentSnapshots);
	$: snapshotMetadata =
		(params?.snapshotMetadata as Record<string, unknown> | undefined) ?? undefined;
	$: currentSnapshot = recentSnapshots.find((s) => s.id === snapshotId);
	$: currentFilename = asString(snapshotMetadata?.originalFilename, currentSnapshot?.filename ?? '-');
	$: currentShortId = snapshotId ? shortHash(snapshotId) : '-';
	$: file_format = (asString(params?.file_format, 'csv') as FileFormat) ?? 'csv';
	$: delimiter = asString(params?.delimiter, file_format === 'tsv' ? '\t' : ',');
	$: delimiterDisplay = delimiter === '\t' ? '\\t' : delimiter;
	$: hasHeaderMode = params?.has_header === true ? 'yes' : params?.has_header === false ? 'no' : 'auto';
	$: quoteChar = asString((params as any)?.quote_char, '"');
	$: escapeChar = asString((params as any)?.escape_char, '\\');
	$: malformedRowPolicy = asString((params as any)?.malformed_row_policy, 'fail');
	$: decimalSeparator = asString((params as any)?.decimal_separator, '.');
	$: thousandsSeparator = asString((params as any)?.thousands_separator, '');
	$: dateColumnsText = Array.isArray((params as any)?.date_columns)
		? ((params as any).date_columns as unknown[])
				.map((value) => asString(value, '').trim())
				.filter((value) => value.length > 0)
				.join(', ')
		: '';
	$: dateFormat = asString((params as any)?.date_format, '');
	$: jsonMode = asString((params as any)?.json_mode, 'auto');
	$: sheet_name = asString(params?.sheet_name, '');
	$: txtRecordMode = asString((params as any)?.txt_record_mode, 'raw');
	$: txtChunkSize = Number((params as any)?.txt_chunk_size ?? 1000);
	$: audioExtractMetadata = asBoolean((params as any)?.audio_extract_metadata, true);
	$: audioNormalize = asBoolean((params as any)?.audio_normalize, false);
	$: audioTargetPeak = Number((params as any)?.audio_target_peak ?? 0.9);
	$: audioTranscodeFormat = asString((params as any)?.audio_transcode_format, '');
	$: videoExtractMetadata = asBoolean((params as any)?.video_extract_metadata, true);
	$: videoFrameMode = asString((params as any)?.video_frame_mode, 'none');
	$: videoFrameIntervalSec = Number((params as any)?.video_frame_interval_sec ?? 1.0);
	$: videoMaxFrames = Number((params as any)?.video_max_frames ?? 5);
	$: encoding = asString(params?.encoding, 'utf-8');
	$: cache_enabled = asBoolean(params?.cache_enabled, true);
	$: activeNodeId = asString(selectedNode?.id, '');
	$: recentAdjustments = activeNodeId ? (recentAdjustmentsByNode.get(activeNodeId) ?? []) : [];
	$: effectiveConfigLines = effectiveConfigForSource('file', params as Record<string, unknown>);
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

	function asNonEmptyString(value: unknown): string | undefined {
		const text = asString(value, '').trim();
		return text.length > 0 ? text : undefined;
	}

	function resolveFilename(overrides: SourceFilePatch = {}): string {
		return (
			asNonEmptyString((overrides as any).filename) ??
			asNonEmptyString((overrides as any)?.snapshotMetadata?.originalFilename) ??
			asNonEmptyString((params as any)?.filename) ??
			'data.txt'
		);
	}

	function canonicalFileParams(overrides: SourceFilePatch = {}): SourceFilePatch {
		const format = (asString(overrides.file_format, file_format) as FileFormat) || 'txt';
		const normalizedRecent = normalizeRecentSnapshots(
			(overrides as any)?.recentSnapshots ?? recentSnapshots,
			(overrides as any)?.recentSnapshotIds
		);
		const sizeRaw = (overrides as any).file_size ?? (params as any)?.file_size;
		const sizeNum = Number(sizeRaw);
		const canonical: SourceFilePatch = {
			snapshotId: asString(overrides.snapshotId, snapshotId).toLowerCase() || undefined,
			snapshotMetadata: (overrides.snapshotMetadata ?? snapshotMetadata) as any,
			recentSnapshots: normalizedRecent,
			recentSnapshotIds: normalizedRecent.map((e) => e.id),
			rel_path: asString((overrides as any).rel_path, asString((params as any)?.rel_path, '.')),
			filename: resolveFilename(overrides),
			file_size: Number.isFinite(sizeNum) && sizeNum >= 0 ? sizeNum : undefined,
			file_mime: asString((overrides as any).file_mime, asString((params as any)?.file_mime, '')) || undefined,
			file_format: format,
			delimiter: format === 'csv' || format === 'tsv' ? asString(overrides.delimiter, delimiter) : undefined,
			sheet_name: format === 'excel' ? asString(overrides.sheet_name, sheet_name) : undefined,
			audio_extract_metadata:
				audioFileFormatOptions.includes(format) ? asBoolean((overrides as any).audio_extract_metadata, audioExtractMetadata) : undefined,
			audio_normalize:
				audioFileFormatOptions.includes(format) ? asBoolean((overrides as any).audio_normalize, audioNormalize) : undefined,
			audio_target_peak:
				audioFileFormatOptions.includes(format)
					? Number((overrides as any).audio_target_peak ?? audioTargetPeak)
					: undefined,
			audio_transcode_format:
				audioFileFormatOptions.includes(format)
					? asString((overrides as any).audio_transcode_format, audioTranscodeFormat) || undefined
					: undefined,
			video_extract_metadata:
				videoFileFormatOptions.includes(format) ? asBoolean((overrides as any).video_extract_metadata, videoExtractMetadata) : undefined,
			video_frame_mode:
				videoFileFormatOptions.includes(format) ? asString((overrides as any).video_frame_mode, videoFrameMode) : undefined,
			video_frame_interval_sec:
				videoFileFormatOptions.includes(format)
					? Number((overrides as any).video_frame_interval_sec ?? videoFrameIntervalSec)
					: undefined,
			video_max_frames:
				videoFileFormatOptions.includes(format)
					? Number((overrides as any).video_max_frames ?? videoMaxFrames)
					: undefined,
			encoding: asString(overrides.encoding, encoding || 'utf-8'),
			cache_enabled: asBoolean(overrides.cache_enabled, cache_enabled),
			output: (overrides.output ?? (params as any)?.output) as any
		};
		return canonical;
	}

	function optionLabel(entry: RecentSnapshot): string {
		return recentOptionLabel(entry, loadingIds.includes(entry.id), shortHash);
	}

	function pushAdjustments(entries: string[]): void {
		if (!activeNodeId || entries.length === 0) return;
		for (const entry of entries) {
			emitSourceEditorTelemetry(
				makeAutoAdjustmentEvent('file', activeNodeId, entry, {
					file_format,
					filename: resolveFilename(),
					snapshotId
				})
			);
		}
		const current = recentAdjustmentsByNode.get(activeNodeId) ?? [];
		const next = [...entries, ...current].slice(0, ADJUSTMENT_LOG_LIMIT);
		recentAdjustmentsByNode.set(activeNodeId, next);
		recentAdjustments = next;
	}

	function patchForDetectedFormat(next: FileFormat | null, detectedFilename?: string): SourceFilePatch {
		if (!next) return {};
		const detectedName = asString(detectedFilename, '').trim().toLowerCase();
		const jsonModeHint: 'ndjson' | undefined =
			detectedName.endsWith('.ndjson') || detectedName.endsWith('.jsonl') ? 'ndjson' : undefined;
		const patch: SourceFilePatch = { file_format: next };
		if (next === 'csv' || next === 'tsv') {
			patch.delimiter = next === 'tsv' ? '\t' : ',';
			patch.has_header = params?.has_header;
			patch.quote_char = asString((params as any)?.quote_char, '"');
			patch.escape_char = asString((params as any)?.escape_char, '\\');
			patch.malformed_row_policy = asString((params as any)?.malformed_row_policy, 'fail') as any;
			patch.decimal_separator = (asString((params as any)?.decimal_separator, '.') as '.' | ',') ?? '.';
			patch.thousands_separator = asString((params as any)?.thousands_separator, '') || undefined;
			patch.date_columns = Array.isArray((params as any)?.date_columns) ? ((params as any)?.date_columns as string[]) : [];
			patch.date_format = asString((params as any)?.date_format, '') || undefined;
			patch.sheet_name = undefined;
			const currentMode = asString((params as any)?.output?.mode, '').trim().toLowerCase();
			// Auto-switch to table for csv/tsv unless user already selected a non-text mode.
			if (currentMode === '' || currentMode === 'text' || currentMode === 'table') {
				patch.output = {
					...(((params as any)?.output as Record<string, unknown> | undefined) ?? {}),
					mode: 'table'
				} as any;
			}
		} else if (next === 'excel') {
			patch.sheet_name = asString(params?.sheet_name, '') || 'Sheet1';
			patch.delimiter = undefined;
			patch.has_header = params?.has_header;
		} else if (next === 'json') {
			patch.json_mode = (jsonModeHint ?? asString((params as any)?.json_mode, 'auto')) as any;
			const currentMode = asString((params as any)?.output?.mode, '').trim().toLowerCase();
			if (currentMode === '' || currentMode === 'text' || currentMode === 'json') {
				patch.output = {
					...(((params as any)?.output as Record<string, unknown> | undefined) ?? {}),
					mode: 'json'
				} as any;
			}
		} else {
			patch.delimiter = undefined;
			patch.has_header = undefined;
			patch.sheet_name = undefined;
		}
		if (audioFileFormatOptions.includes(next)) {
			patch.audio_extract_metadata = asBoolean((params as any)?.audio_extract_metadata, true);
			patch.audio_normalize = asBoolean((params as any)?.audio_normalize, false);
			patch.audio_target_peak = Number((params as any)?.audio_target_peak ?? 0.9);
			patch.audio_transcode_format =
				asString((params as any)?.audio_transcode_format, '').trim().toLowerCase() || undefined;
		}
		if (videoFileFormatOptions.includes(next)) {
			patch.video_extract_metadata = asBoolean((params as any)?.video_extract_metadata, true);
			patch.video_frame_mode = asString((params as any)?.video_frame_mode, 'none') as any;
			patch.video_frame_interval_sec = Number((params as any)?.video_frame_interval_sec ?? 1.0);
			patch.video_max_frames = Number((params as any)?.video_max_frames ?? 5);
		}
		return patch;
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
			const detectedPatch = patchForDetectedFormat(
				detectFileFormatFromFilename(incoming.filename ?? file.name),
				incoming.filename ?? file.name
			);
			const patch: SourceFilePatch = {
				snapshotId: incoming.id,
				snapshotMetadata: result.metadata,
				filename: incoming.filename ?? file.name,
				...detectedPatch,
				...withRecentPatch(nextRecent)
			};
			pushAdjustments(fileAutoAdjustmentNotices(params, detectedPatch, 'Upload auto-adjustment'));
			commitSnapshot(canonicalFileParams(patch));
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

	async function onSelectPrevious(value: string): Promise<void> {
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
		const detectedPatch = patchForDetectedFormat(
			detectFileFormatFromFilename(resolved?.filename),
			resolved?.filename
		);
		const patch: SourceFilePatch = {
			snapshotId: sid,
			snapshotMetadata: {
				snapshotId: sid,
				originalFilename: resolved?.filename,
				importedAt: resolved?.importedAt,
				byteSize: resolved?.size,
				mimeType: resolved?.mimeType
			},
			filename: resolved?.filename,
			...detectedPatch,
			...withRecentPatch(nextRecent)
		};
		pushAdjustments(fileAutoAdjustmentNotices(params, detectedPatch, 'Snapshot auto-adjustment'));
		commitSnapshot(canonicalFileParams(patch));
	}

	function setFileFormat(nextFormat: string): void {
		if (!fileFormatOptions.includes(nextFormat as FileFormat)) return;
		const patch = patchForDetectedFormat(nextFormat as FileFormat);
		pushAdjustments(fileAutoAdjustmentNotices(params, patch, 'Format auto-adjustment'));
		draft(patch);
		commit(patch);
	}

	function decodeDelimiterInput(raw: string): string {
		if (raw === '\\t') return '\t';
		return raw;
	}
</script>

{#if selectedNode}
	<div class="sourceFileEditor">
	<Section title="File">
		<SourceCapabilityBanner sourceKind="file" params={params as Record<string, unknown>} />
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
				<div class="dropTitle">{isUploading ? 'Uploading...' : 'Choose a file'}</div>
				<div class="dropHint">or drag & drop here</div>
				<input bind:this={fileInputEl} type="file" hidden on:change={onFileInputChange} />
			</div>
			{#if uploadError}
				<div class="error">{uploadError}</div>
			{/if}
		</Field>

		<Field label="current file snapshot" stacked>
			<div class="snapshotKv">
				<div class="kvRow">
					<div class="kvKey">file</div>
					<div class="kvVal">{currentFilename}</div>
				</div>
				<div class="kvRow">
					<div class="kvKey">id</div>
					<div class="kvVal mono">{currentShortId}</div>
				</div>
				<div class="kvRow">
					<div class="kvKey">size</div>
					<div class="kvVal">{bytesLabel(snapshotMetadata?.byteSize ?? currentSnapshot?.size)}</div>
				</div>
				<div class="kvRow">
					<div class="kvKey">imported</div>
					<div class="kvVal">{asString(snapshotMetadata?.importedAt, currentSnapshot?.importedAt ?? '-')}</div>
				</div>
			</div>
		</Field>

		<Field label="previous uploads" stacked>
			<ThemedSelect
				value={snapshotId || ''}
				options={previousUploadOptions(displayRecentSnapshots)}
				ariaLabel="previous uploads"
				onValueChange={onSelectPrevious}
			/>
		</Field>

		<Field label="file format">
			<ThemedSelect
				value={file_format}
				options={fileFormatSelectOptions}
				ariaLabel="file format"
				onValueChange={setFileFormat}
			/>
		</Field>

		{#if file_format === 'csv' || file_format === 'tsv'}
			<Field label="delimiter">
				<Input
					value={delimiterDisplay}
					placeholder={file_format === 'tsv' ? '\\t' : ','}
					onInput={(event) =>
						draft({ delimiter: decodeDelimiterInput((event.currentTarget as HTMLInputElement).value) })}

				/>
			</Field>
			<Field label="first row is header">
				<ThemedSelect
					value={hasHeaderMode}
					options={hasHeaderOptions}
					ariaLabel="csv header mode"
					onValueChange={(value) => {
						const has_header = value === 'yes' ? true : value === 'no' ? false : undefined;
						draft({ has_header });
						commit({ has_header });
					}}
				/>
			</Field>
			<Field label="quote character">
				<Input
					value={quoteChar}
					placeholder={'"'}
					onInput={(event) => draft({ quote_char: (event.currentTarget as HTMLInputElement).value.slice(0, 1) || undefined })}

				/>
			</Field>
			<Field label="escape character">
				<Input
					value={escapeChar}
					placeholder={'\\'}
					onInput={(event) => draft({ escape_char: (event.currentTarget as HTMLInputElement).value.slice(0, 1) || undefined })}

				/>
			</Field>
			<Field label="malformed row policy">
				<ThemedSelect
					value={malformedRowPolicy}
					options={malformedRowOptions}
					ariaLabel="csv malformed row policy"
					onValueChange={(value) => {
						draft({ malformed_row_policy: value as any });
						commit({ malformed_row_policy: value as any });
					}}
				/>
			</Field>
			<Field label="decimal separator">
				<ThemedSelect
					value={decimalSeparator}
					options={decimalSeparatorOptions}
					ariaLabel="csv decimal separator"
					onValueChange={(value) => {
						draft({ decimal_separator: value as any });
						commit({ decimal_separator: value as any });
					}}
				/>
			</Field>
			<Field label="thousands separator">
				<Input
					value={thousandsSeparator}
					placeholder=","
					onInput={(event) => draft({ thousands_separator: (event.currentTarget as HTMLInputElement).value.slice(0, 1) || undefined })}

				/>
			</Field>
			<Field label="date columns (comma-separated)">
				<Input
					value={dateColumnsText}
					placeholder="date,created_at"
					onInput={(event) => {
						const values = (event.currentTarget as HTMLInputElement).value
							.split(',')
							.map((v) => v.trim())
							.filter((v) => v.length > 0);
						draft({ date_columns: values as any });
					}}

				/>
			</Field>
			<Field label="date format">
				<Input
					value={dateFormat}
					placeholder="%Y-%m-%d"
					onInput={(event) => draft({ date_format: (event.currentTarget as HTMLInputElement).value || undefined })}

				/>
			</Field>
		{/if}

		{#if file_format === 'json'}
			<Field label="json mode">
				<ThemedSelect
					value={jsonMode}
					options={jsonModeOptions}
					ariaLabel="json mode"
					onValueChange={(value) => {
						draft({ json_mode: value as any });
						commit({ json_mode: value as any });
					}}
				/>
			</Field>
		{/if}

		{#if file_format === 'excel'}
			<Field label="first row is header">
				<ThemedSelect
					value={hasHeaderMode}
					options={hasHeaderOptions}
					ariaLabel="excel header mode"
					onValueChange={(value) => {
						const has_header = value === 'yes' ? true : value === 'no' ? false : undefined;
						draft({ has_header });
						commit({ has_header });
					}}
				/>
			</Field>
			<Field label="sheet_name">
				<Input
					value={sheet_name}
					placeholder="Sheet1 (blank = first sheet)"
					onInput={(event) => {
						const value = (event.currentTarget as HTMLInputElement).value;
						draft({ sheet_name: value.trim() === '' ? undefined : value });
					}}

				/>
			</Field>
		{/if}

		{#if file_format === 'txt'}
			<Field label="record mode">
				<ThemedSelect
					value={txtRecordMode}
					options={txtRecordModeOptions}
					ariaLabel="text record mode"
					onValueChange={(value) => {
						draft({ txt_record_mode: value as any });
						commit({ txt_record_mode: value as any });
					}}
				/>
			</Field>
			{#if txtRecordMode === 'fixed_chunk'}
				<Field label="chunk size">
					<Input
						type="number"
						min="1"
						value={Number.isFinite(txtChunkSize) ? String(txtChunkSize) : '1000'}
						onInput={(event) => {
							const raw = Number((event.currentTarget as HTMLInputElement).value);
							draft({ txt_chunk_size: Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 1000 });
						}}

					/>
				</Field>
			{/if}
		{/if}

		{#if audioFileFormatOptions.includes(file_format)}
			<Field label="extract metadata">
				<Input
					type="checkbox"
					checked={audioExtractMetadata}
					onChange={(event) => {
						const checked = (event.currentTarget as HTMLInputElement).checked;
						draft({ audio_extract_metadata: checked as any });
						commit({ audio_extract_metadata: checked as any });
					}}
				/>
			</Field>
			<Field label="normalize audio">
				<Input
					type="checkbox"
					checked={audioNormalize}
					onChange={(event) => {
						const checked = (event.currentTarget as HTMLInputElement).checked;
						draft({ audio_normalize: checked as any });
						commit({ audio_normalize: checked as any });
					}}
				/>
			</Field>
			{#if audioNormalize}
				<Field label="target peak (0.01-1.0)">
					<Input
						type="number"
						min="0.01"
						max="1"
						step="0.01"
						value={Number.isFinite(audioTargetPeak) ? String(audioTargetPeak) : '0.9'}
						onInput={(event) => {
							const raw = Number((event.currentTarget as HTMLInputElement).value);
							const value = Number.isFinite(raw) ? Math.max(0.01, Math.min(1, raw)) : 0.9;
							draft({ audio_target_peak: value as any });
						}}

					/>
				</Field>
			{/if}
			<Field label="transcode format">
				<ThemedSelect
					value={audioTranscodeFormat || ''}
					options={audioTranscodeOptions}
					ariaLabel="audio transcode format"
					onValueChange={(raw) => {
						const value = asString(raw, '');
						draft({ audio_transcode_format: value || undefined });
						commit({ audio_transcode_format: value || undefined });
					}}
				/>
			</Field>
		{/if}

		{#if videoFileFormatOptions.includes(file_format)}
			<Field label="extract metadata">
				<Input
					type="checkbox"
					checked={videoExtractMetadata}
					onChange={(event) => {
						const checked = (event.currentTarget as HTMLInputElement).checked;
						draft({ video_extract_metadata: checked as any });
						commit({ video_extract_metadata: checked as any });
					}}
				/>
			</Field>
			<Field label="frame extraction mode">
				<ThemedSelect
					value={videoFrameMode}
					options={videoFrameModeOptions}
					ariaLabel="video frame extraction mode"
					onValueChange={(raw) => {
						const value = asString(raw, 'none');
						draft({ video_frame_mode: value as any });
						commit({ video_frame_mode: value as any });
					}}
				/>
			</Field>
			{#if videoFrameMode === 'interval'}
				<Field label="frame interval sec">
					<Input
						type="number"
						min="0.01"
						step="0.01"
						value={Number.isFinite(videoFrameIntervalSec) ? String(videoFrameIntervalSec) : '1'}
						onInput={(event) => {
							const raw = Number((event.currentTarget as HTMLInputElement).value);
							const value = Number.isFinite(raw) && raw > 0 ? raw : 1;
							draft({ video_frame_interval_sec: value as any });
						}}

					/>
				</Field>
			{/if}
			{#if videoFrameMode !== 'none'}
				<Field label="max frames">
					<Input
						type="number"
						min="1"
						step="1"
						value={Number.isFinite(videoMaxFrames) ? String(Math.max(1, Math.floor(videoMaxFrames))) : '5'}
						onInput={(event) => {
							const raw = Number((event.currentTarget as HTMLInputElement).value);
							const value = Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 5;
							draft({ video_max_frames: value as any });
						}}

					/>
				</Field>
			{/if}
		{/if}

		<Field label="encoding">
			<ThemedSelect
				value={encoding || 'utf-8'}
				options={encodingOptions}
				ariaLabel="file encoding"
				onValueChange={(value) => {
					draft({ encoding: value });
					commit({ encoding: value });
				}}
			/>
		</Field>

		<Field label="cache enabled">
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

		<SourceEffectivePreview lines={effectiveConfigLines} recentAdjustments={recentAdjustments} />
	</Section>
	</div>
{/if}

<style>
	@import '../../../styles/editorCommon.css';

	.sourceFileEditor .dropzone {
		border: 1px dashed rgba(148, 163, 184, 0.7);
		border-radius: 10px;
		padding: 10px;
		display: grid;
		gap: 6px;
	}
	.sourceFileEditor .dropzone.dragOver {
		border-color: rgba(56, 189, 248, 0.9);
		background: rgba(15, 23, 42, 0.35);
	}
	.sourceFileEditor .dropTitle {
		font-weight: 600;
	}
	.sourceFileEditor .dropHint {
		font-size: 12px;
		opacity: 0.9;
	}
	.sourceFileEditor .error {
		margin-top: 6px;
		font-size: 12px;
		color: #f87171;
	}
	.sourceFileEditor .snapshotKv {
		display: grid;
		font-size: 12px;
	}
	.sourceFileEditor .kvRow {
		display: grid;
		grid-template-columns: max-content minmax(0, 1fr);
		gap: 8px;
		align-items: start;
	}
	.sourceFileEditor .kvKey {
		opacity: 0.7;
	}
	.sourceFileEditor .kvVal {
		overflow-wrap: anywhere;
		word-break: normal;
	}
</style>
