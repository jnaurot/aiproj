<script lang="ts">
	import JsonTreeNode from './JsonTreeNode.svelte';

	export let artifactId: string;
	export let mimeType: string | undefined;
	export let preview: string | undefined;

	type ArtifactMeta = {
		artifactId: string;
		nodeKind: string;
		mimeType: string;
		sizeBytes: number;
		createdAt: string;
		paramsHash: string;
		upstreamCount: number;
		payloadSchema?: Record<string, any> | null;
	};

	type TableCol = { name: string; type: string };

	let loading = false;
	let error: string | null = null;
	let meta: ArtifactMeta | null = null;
	let text: string | null = null;
	let jsonObj: any = null;

	let tableRows: Record<string, any>[] = [];
	let tableCols: TableCol[] = [];
	let totalRows = 0;
	let offset = 0;
	let limit = 100;
	let jsonEstimatedSize = 0;

	$: if (artifactId) void loadArtifact(artifactId);

	$: effectiveMime = meta?.mimeType ?? mimeType ?? '';
	$: isTable =
		((meta?.payloadSchema as any)?.type === 'table') ||
		effectiveMime.includes('text/csv') ||
		effectiveMime.includes('text/tab-separated-values') ||
		effectiveMime.includes('parquet') ||
		effectiveMime.includes('excel');
	$: isJson = effectiveMime.includes('application/json') || (meta?.payloadSchema as any)?.type === 'json';
	$: isMarkdown = effectiveMime.includes('text/markdown');
	$: colCount = tableCols.length || ((meta?.payloadSchema as any)?.columns?.length ?? 0);

	function typedBadge(raw: string | undefined) {
		const t = String(raw ?? 'unknown').toLowerCase();
		if (t.includes('int')) return 'int';
		if (t.includes('float') || t.includes('double') || t.includes('decimal')) return 'float';
		if (t.includes('bool')) return 'bool';
		if (t.includes('date') || t.includes('time')) return 'date';
		if (t.includes('str') || t.includes('text') || t.includes('object')) return 'string';
		return 'unknown';
	}

	function escapeHtml(s: string): string {
		return s
			.replaceAll('&', '&amp;')
			.replaceAll('<', '&lt;')
			.replaceAll('>', '&gt;')
			.replaceAll('"', '&quot;')
			.replaceAll("'", '&#39;');
	}

	function inlineMd(s: string): string {
		return s
			.replace(
				/\[([^\]]+)\]\(([^)\s]+)(?:\s+"([^"]*)")?\)/g,
				(_m, label, href, title) =>
					`<a href="${href}"${title ? ` title="${title}"` : ''}>${label}</a>`
			)
			.replace(/`([^`]+)`/g, '<code>$1</code>')
			.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
			.replace(/\*([^*]+)\*/g, '<em>$1</em>');
	}

	function renderMarkdown(md: string): string {
		const lines = md.replace(/\r\n/g, '\n').split('\n');
		const out: string[] = [];
		let inCode = false;
		let inList = false;

		for (const raw of lines) {
			const line = escapeHtml(raw);
			if (line.startsWith('```')) {
				if (!inCode) {
					if (inList) {
						out.push('</ul>');
						inList = false;
					}
					out.push('<pre><code>');
					inCode = true;
				} else {
					out.push('</code></pre>');
					inCode = false;
				}
				continue;
			}
			if (inCode) {
				out.push(line);
				continue;
			}
			const h = line.match(/^(#{1,6})\s+(.*)$/);
			if (h) {
				if (inList) {
					out.push('</ul>');
					inList = false;
				}
				const level = h[1].length;
				out.push(`<h${level}>${inlineMd(h[2])}</h${level}>`);
				continue;
			}
			const li = line.match(/^-\s+(.*)$/);
			if (li) {
				if (!inList) {
					out.push('<ul>');
					inList = true;
				}
				out.push(`<li>${inlineMd(li[1])}</li>`);
				continue;
			}
			if (!line.trim()) {
				if (inList) {
					out.push('</ul>');
					inList = false;
				}
				continue;
			}
			if (inList) {
				out.push('</ul>');
				inList = false;
			}
			out.push(`<p>${inlineMd(line)}</p>`);
		}
		if (inList) out.push('</ul>');
		if (inCode) out.push('</code></pre>');
		return out.join('\n');
	}

	function parseCharset(contentType: string): string {
		const m = /charset=([^;]+)/i.exec(contentType ?? '');
		return (m?.[1] ?? 'utf-8').trim().replace(/^"|"$/g, '');
	}

	function decodeBytes(buf: ArrayBuffer, contentType: string): string {
		const charset = parseCharset(contentType);
		let text = '';
		try {
			text = new TextDecoder(charset, { fatal: false }).decode(buf);
		} catch {
			text = new TextDecoder('utf-8', { fatal: false }).decode(buf);
		}
		return text.replace(/^\uFEFF/, '');
	}

	function safeHref(raw: string): string | null {
		const href = String(raw ?? '').trim();
		if (!href) return null;
		try {
			const parsed = new URL(href, 'http://localhost');
			const proto = parsed.protocol.toLowerCase();
			if (proto === 'http:' || proto === 'https:' || proto === 'mailto:') return parsed.href;
			return null;
		} catch {
			return null;
		}
	}

	function sanitizeHtml(unsafe: string): string {
		const parser = new DOMParser();
		const doc = parser.parseFromString(`<div>${unsafe}</div>`, 'text/html');
		const root = doc.body.firstElementChild as HTMLElement | null;
		if (!root) return '';

		const allowedTags = new Set([
			'P',
			'H1',
			'H2',
			'H3',
			'H4',
			'H5',
			'H6',
			'UL',
			'OL',
			'LI',
			'STRONG',
			'EM',
			'CODE',
			'PRE',
			'A',
			'BLOCKQUOTE',
			'TABLE',
			'THEAD',
			'TBODY',
			'TR',
			'TH',
			'TD',
			'BR',
			'HR'
		]);

		const walk = (node: Node) => {
			if (node.nodeType === Node.ELEMENT_NODE) {
				const el = node as HTMLElement;
				const tag = el.tagName.toUpperCase();

				if (!allowedTags.has(tag)) {
					const parent = el.parentNode;
					if (parent) {
						while (el.firstChild) parent.insertBefore(el.firstChild, el);
						parent.removeChild(el);
					}
					return;
				}

				const attrs = Array.from(el.attributes);
				for (const attr of attrs) {
					const name = attr.name.toLowerCase();
					if (name.startsWith('on')) {
						el.removeAttribute(attr.name);
						continue;
					}
					if (tag !== 'A') {
						el.removeAttribute(attr.name);
						continue;
					}
					if (name !== 'href' && name !== 'title') {
						el.removeAttribute(attr.name);
					}
				}

				if (tag === 'A') {
					const href = safeHref(el.getAttribute('href') ?? '');
					if (!href) {
						const parent = el.parentNode;
						if (parent) {
							while (el.firstChild) parent.insertBefore(el.firstChild, el);
							parent.removeChild(el);
						}
						return;
					}
					el.setAttribute('href', href);
					el.setAttribute('rel', 'noopener noreferrer');
					el.setAttribute('target', '_blank');
				}
			}
			const children = Array.from(node.childNodes);
			for (const child of children) walk(child);
		};

		walk(root);
		return root.innerHTML;
	}

	function renderSafeMarkdown(md: string): string {
		return sanitizeHtml(renderMarkdown(md));
	}

	async function copyArtifactId() {
		try {
			await navigator.clipboard.writeText(artifactId);
		} catch {}
	}

	async function loadArtifact(id: string) {
		loading = true;
		error = null;
		text = null;
		jsonObj = null;
		tableRows = [];
		tableCols = [];
		totalRows = 0;
		offset = 0;

		try {
			const m = await fetch(`/runs/artifacts/${encodeURIComponent(id)}/meta`);
			if (!m.ok) throw new Error(`${m.status} ${m.statusText}`);
			meta = await m.json();

			if (isTableLike(meta?.mimeType ?? mimeType ?? '', meta?.payloadSchema)) {
				await loadTablePage(0);
				return;
			}

			const res = await fetch(`/runs/artifacts/${encodeURIComponent(id)}`);
			if (!res.ok) {
				const body = await res.text().catch(() => '');
				throw new Error(`${res.status} ${res.statusText} ${body}`);
			}

			const ct = res.headers.get('content-type') ?? meta?.mimeType ?? mimeType ?? '';
			const raw = await res.arrayBuffer();
			const decoded = decodeBytes(raw, ct);
			if (ct.includes('application/json')) {
				try {
					jsonObj = JSON.parse(decoded);
					jsonEstimatedSize = decoded.length;
				} catch {
					text = decoded;
					jsonEstimatedSize = 0;
				}
			} else {
				text = decoded;
				jsonEstimatedSize = 0;
			}
		} catch (e: any) {
			error = e?.message ?? String(e);
		} finally {
			loading = false;
		}
	}

	$: jsonRootOpen = jsonEstimatedSize > 0 && jsonEstimatedSize <= 200_000;

	function isTableLike(ct: string, payloadSchema: Record<string, any> | null | undefined): boolean {
		const t = String(ct ?? '').toLowerCase();
		return (
			(payloadSchema as any)?.type === 'table' ||
			t.includes('text/csv') ||
			t.includes('tab-separated-values') ||
			t.includes('parquet') ||
			t.includes('excel')
		);
	}

	async function loadTablePage(nextOffset: number) {
		loading = true;
		error = null;
		try {
			const res = await fetch(
				`/runs/artifacts/${encodeURIComponent(artifactId)}/preview?offset=${nextOffset}&limit=${limit}`
			);
			if (!res.ok) {
				const body = await res.text().catch(() => '');
				throw new Error(`${res.status} ${res.statusText} ${body}`);
			}
			const page = await res.json();
			offset = Number(page.offset ?? nextOffset);
			totalRows = Number(page.totalRows ?? 0);
			tableCols = Array.isArray(page.columns) ? page.columns : [];
			tableRows = Array.isArray(page.rows) ? page.rows : [];
		} catch (e: any) {
			error = e?.message ?? String(e);
		} finally {
			loading = false;
		}
	}

	function nextPage() {
		const n = offset + limit;
		if (n < totalRows) void loadTablePage(n);
	}
	function prevPage() {
		const n = Math.max(0, offset - limit);
		void loadTablePage(n);
	}
	function applyPageSize(next: number) {
		limit = next;
		void loadTablePage(0);
	}

	async function downloadArtifact() {
		try {
			const res = await fetch(`/runs/artifacts/${encodeURIComponent(artifactId)}`);
			if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
			const blob = await res.blob();
			const url = URL.createObjectURL(blob);
			const a = document.createElement('a');
			a.href = url;
			a.download = `${artifactId.slice(0, 12)}`;
			document.body.appendChild(a);
			a.click();
			a.remove();
			URL.revokeObjectURL(url);
		} catch (e: any) {
			error = e?.message ?? String(e);
		}
	}
</script>

{#if preview}
	<div class="block">
		<div class="label">Preview</div>
		<pre>{preview}</pre>
	</div>
{/if}

<div class="chips">
	<span class="chip"><b>mime</b> {effectiveMime || '-'}</span>
	<span class="chip"><b>rows</b> {isTable ? totalRows || '-' : '-'}</span>
	<span class="chip"><b>cols</b> {isTable ? colCount || '-' : '-'}</span>
	<button class="download" on:click={downloadArtifact}>Download</button>
</div>

<div class="meta">
	<div class="artifactRow">
		<b>artifactId:</b>
		<button class="copyBtn" on:click={copyArtifactId}>Copy</button>
		<span class="artifactId">{artifactId}</span>
	</div>
</div>

{#if loading}
	<div class="muted">Loading artifact...</div>
{:else if error}
	<div class="error">{error}</div>
{:else if isTable}
	<div class="pager">
		<button on:click={prevPage} disabled={offset <= 0}>Prev</button>
		<button on:click={nextPage} disabled={offset + limit >= totalRows}>Next</button>
		<span>{offset + 1}-{Math.min(offset + limit, totalRows)} / {totalRows}</span>
		<select value={String(limit)} on:change={(e) => applyPageSize(parseInt((e.currentTarget as HTMLSelectElement).value, 10))}>
			<option value="25">25</option>
			<option value="50">50</option>
			<option value="100">100</option>
			<option value="200">200</option>
		</select>
	</div>
	<div class="tableWrap">
		<table>
			<thead>
				<tr>
					{#each tableCols as c}
						<th>
							<div class="col">{c.name}</div>
							<span class="badge">{typedBadge(c.type)}</span>
						</th>
					{/each}
				</tr>
			</thead>
			<tbody>
				{#each tableRows as row}
					<tr>
						{#each tableCols as c}
							<td>{String(row[c.name] ?? '')}</td>
						{/each}
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{:else if jsonObj}
	<div class="block">
		<div class="label">JSON</div>
		<div class="jsonTools">
			<button on:click={() => navigator.clipboard.writeText(JSON.stringify(jsonObj, null, 2))}>Copy</button>
		</div>
		<details open={jsonRootOpen}>
			<summary>Root</summary>
			<JsonTreeNode value={jsonObj} />
		</details>
	</div>
{:else if text !== null}
	<div class="block">
		<div class="label">{isMarkdown ? 'Markdown' : 'Content'}</div>
		{#if isMarkdown}
			<div class="markdown">{@html renderSafeMarkdown(text)}</div>
		{:else}
			<pre>{text}</pre>
		{/if}
	</div>
{:else}
	<div class="muted">No content.</div>
{/if}

<style>
	:global(:root) {
		--av-text: #0f172a;
		--av-muted: #4b5563;
		--av-border: #d1d5db;
		--av-surface: #ffffff;
		--av-surface-alt: #f8fafc;
		--av-chip-bg: #f3f4f6;
		--av-chip-text: #111827;
		--av-badge-bg: #e0e7ff;
		--av-badge-text: #1f2937;
		--av-code-bg: #f3f4f6;
	}
	@media (prefers-color-scheme: dark) {
		:global(:root) {
			--av-text: #e5e7eb;
			--av-muted: #9ca3af;
			--av-border: #374151;
			--av-surface: #0b1220;
			--av-surface-alt: #111827;
			--av-chip-bg: #1f2937;
			--av-chip-text: #e5e7eb;
			--av-badge-bg: #24324a;
			--av-badge-text: #dbeafe;
			--av-code-bg: #111827;
		}
	}
	.meta,
	.block,
	.label,
	.pager,
	.tableWrap,
	.markdown,
	pre,
	code {
		color: var(--av-text);
	}
	.meta {
		margin: 8px 0 12px;
		font-size: 12px;
		opacity: 0.9;
	}
	.block {
		margin-top: 10px;
	}
	.label {
		font-weight: 700;
		margin-bottom: 6px;
	}
	.muted {
		color: var(--av-muted);
	}
	.error {
		color: #b00020;
		white-space: pre-wrap;
	}
	pre {
		white-space: pre-wrap;
		word-break: break-word;
	}
	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
		align-items: center;
		margin: 8px 0 10px;
	}
	.chip {
		padding: 4px 8px;
		border-radius: 999px;
		border: 1px solid var(--av-border);
		font-size: 12px;
		background: var(--av-chip-bg);
		color: var(--av-chip-text);
	}
	.download {
		margin-left: auto;
	}
	.download,
	.pager button,
	.pager select,
	.copyBtn {
		background: var(--av-surface-alt);
		color: var(--av-text);
		border: 1px solid var(--av-border);
		border-radius: 6px;
	}
	.artifactRow {
		display: flex;
		align-items: center;
		gap: 8px;
		flex-wrap: wrap;
	}
	.artifactId {
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New',
			monospace;
		word-break: break-all;
	}
	.pager {
		display: flex;
		gap: 8px;
		align-items: center;
		margin: 8px 0;
	}
	.tableWrap {
		overflow: auto;
		max-height: 420px;
		border: 1px solid var(--av-border);
		border-radius: 8px;
		background: var(--av-surface);
	}
	table {
		border-collapse: collapse;
		width: 100%;
		font-size: 12px;
		background: var(--av-surface);
	}
	th,
	td {
		padding: 6px 8px;
		border-bottom: 1px solid var(--av-border);
		text-align: left;
		vertical-align: top;
		color: var(--av-text);
	}
	th {
		position: sticky;
		top: 0;
		background: var(--av-surface-alt);
	}
	.badge {
		display: inline-block;
		margin-top: 2px;
		padding: 2px 6px;
		border-radius: 999px;
		background: var(--av-badge-bg);
		color: var(--av-badge-text);
		font-size: 11px;
	}
	.markdown :global(h1),
	.markdown :global(h2),
	.markdown :global(h3) {
		margin: 8px 0 6px;
	}
	.markdown :global(p) {
		margin: 6px 0;
	}
	.markdown :global(pre) {
		background: var(--av-code-bg);
		color: var(--av-text);
		padding: 8px;
		border-radius: 6px;
	}
</style>
