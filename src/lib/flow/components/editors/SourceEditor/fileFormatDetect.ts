import type { SourceFileParams } from '$lib/flow/schema/source';

type FileFormat = SourceFileParams['file_format'];

export function detectFileFormatFromFilename(filename: string | undefined): FileFormat | null {
	const name = String(filename ?? '')
		.trim()
		.toLowerCase();
	if (!name) return null;
	if (name.endsWith('.csv')) return 'csv';
	if (name.endsWith('.tsv')) return 'tsv';
	if (name.endsWith('.parquet')) return 'parquet';
	if (name.endsWith('.json')) return 'json';
	if (name.endsWith('.xlsx') || name.endsWith('.xls')) return 'excel';
	if (name.endsWith('.pdf')) return 'pdf';
	if (name.endsWith('.jpg')) return 'jpg';
	if (name.endsWith('.jpeg')) return 'jpeg';
	if (name.endsWith('.png')) return 'png';
	if (name.endsWith('.webp')) return 'webp';
	if (name.endsWith('.gif')) return 'gif';
	if (name.endsWith('.svg')) return 'svg';
	if (name.endsWith('.tif')) return 'tif';
	if (name.endsWith('.tiff')) return 'tiff';
	if (name.endsWith('.mp3')) return 'mp3';
	if (name.endsWith('.wav')) return 'wav';
	if (name.endsWith('.flac')) return 'flac';
	if (name.endsWith('.ogg')) return 'ogg';
	if (name.endsWith('.m4a')) return 'm4a';
	if (name.endsWith('.aac')) return 'aac';
	if (name.endsWith('.mp4')) return 'mp4';
	if (name.endsWith('.mov')) return 'mov';
	if (name.endsWith('.webm')) return 'webm';
	// doc/docx are handled as text with current backend support.
	if (name.endsWith('.docx') || name.endsWith('.doc')) return 'txt';
	if (name.endsWith('.txt') || name.endsWith('.md') || name.endsWith('.log')) return 'txt';
	return null;
}
