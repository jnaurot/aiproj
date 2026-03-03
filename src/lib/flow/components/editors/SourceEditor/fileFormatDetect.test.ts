import { describe, expect, it } from 'vitest';

import { detectFileFormatFromFilename } from './fileFormatDetect';

describe('detectFileFormatFromFilename', () => {
	it('maps recommended image extensions to image file formats', () => {
		expect(detectFileFormatFromFilename('photo.jpg')).toBe('jpg');
		expect(detectFileFormatFromFilename('photo.jpeg')).toBe('jpeg');
		expect(detectFileFormatFromFilename('image.png')).toBe('png');
		expect(detectFileFormatFromFilename('image.webp')).toBe('webp');
		expect(detectFileFormatFromFilename('anim.gif')).toBe('gif');
		expect(detectFileFormatFromFilename('vector.svg')).toBe('svg');
		expect(detectFileFormatFromFilename('scan.tif')).toBe('tif');
		expect(detectFileFormatFromFilename('scan.tiff')).toBe('tiff');
	});

	it('maps existing text/document formats and returns null for unknown', () => {
		expect(detectFileFormatFromFilename('report.pdf')).toBe('pdf');
		expect(detectFileFormatFromFilename('song.mp3')).toBe('mp3');
		expect(detectFileFormatFromFilename('song.wav')).toBe('wav');
		expect(detectFileFormatFromFilename('song.flac')).toBe('flac');
		expect(detectFileFormatFromFilename('song.ogg')).toBe('ogg');
		expect(detectFileFormatFromFilename('song.m4a')).toBe('m4a');
		expect(detectFileFormatFromFilename('song.aac')).toBe('aac');
		expect(detectFileFormatFromFilename('clip.mp4')).toBe('mp4');
		expect(detectFileFormatFromFilename('clip.mov')).toBe('mov');
		expect(detectFileFormatFromFilename('clip.webm')).toBe('webm');
		expect(detectFileFormatFromFilename('notes.docx')).toBe('txt');
		expect(detectFileFormatFromFilename('notes.doc')).toBe('txt');
		expect(detectFileFormatFromFilename('data.unknown')).toBeNull();
	});
});
