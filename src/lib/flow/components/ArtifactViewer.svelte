<script lang="ts">
	import { onDestroy } from 'svelte';
	import JsonTreeNode from './JsonTreeNode.svelte';
	import {
		getArtifactConsumersUrl,
		getArtifactLineageUrl,
		getArtifactMetaUrl,
		getArtifactPreviewUrl,
		getArtifactUrl
	} from '$lib/flow/client/runs';
	import {
		type AnalysisArtifact,
		extractAnalysisArtifacts
	} from '$lib/flow/components/artifactAnalysis';
	import { extractComponentWrapperOutputs } from '$lib/flow/components/artifactWrapper';

export let artifactId: string;
export let graphId: string;
export let mimeType: string | undefined;
export let payloadType: string | undefined;
export let cached: boolean | undefined = undefined;
export let cacheDecision:
	| 'cache_hit'
	| 'cache_miss'
	| 'cache_hit_contract_mismatch'
	| undefined = undefined;
export let preview: string | undefined;
export let onJumpToNode: ((nodeId: string) => void) | undefined = undefined;

	type ArtifactMeta = {
		artifactId: string;
		nodeKind: string;
		mimeType: string;
		payloadType?: string | null;
		sizeBytes: number;
		contentHash?: string | null;
		createdAt: string;
		paramsHash: string;
		upstreamCount: number;
		upstreamArtifactIds?: string[];
		inputArtifactIds?: string[];
		producerNodeId?: string | null;
		producerRunId?: string | null;
		producerExecKey?: string | null;
		inputRefs?: { artifactId: string; label?: string }[];
		payloadSchema?: Record<string, any> | null;
		builtinEnvironment?: {
			profileId?: string;
			source?: string;
			packages?: string[];
		} | null;
	};

	type InputArtifactSummary = {
		label: string;
		artifactId: string;
		nodeKind: string;
		mimeType: string;
		rows: string | number;
		cols: string | number;
	};

	type ConsumerSummary = {
		inputArtifactId: string;
		consumerRunId: string;
		consumerNodeId: string;
		consumerExecKey?: string | null;
		outputArtifactId: string;
		createdAt: string;
	};

	type TableCol = { name: string; type: string };

	let loading = false;
	let error: string | null = null;
	let meta: ArtifactMeta | null = null;
	let text: string | null = null;
	let jsonObj: any = null;
	let analysisArtifacts: AnalysisArtifact[] = [];
	let imageUrl: string | null = null;
	let audioUrl: string | null = null;
	let videoUrl: string | null = null;

	let tableRows: Record<string, any>[] = [];
	let tableCols: TableCol[] = [];
	let totalRows = 0;
	let offset = 0;
	let limit = 100;
	let jsonEstimatedSize = 0;
	let inputArtifacts: InputArtifactSummary[] = [];
	let consumers: ConsumerSummary[] = [];
	let activeArtifactId = '';
	let boundArtifactId = '';
	let lineageJson: any = null;

	$: if (artifactId && artifactId !== boundArtifactId) {
		boundArtifactId = artifactId;
		activeArtifactId = artifactId;
		void loadArtifact(activeArtifactId);
	}

	$: effectiveMime = meta?.mimeType ?? mimeType ?? '';
	$: effectivePayloadType = meta?.payloadType ?? payloadType ?? '-';
	$: payloadSchemaType = String((meta?.payloadSchema as any)?.type ?? '-');
	$: hasPayloadSchema = Boolean(meta?.payloadSchema && typeof meta.payloadSchema === 'object');
	$: isTable =
		((meta?.payloadSchema as any)?.type === 'table') ||
		effectiveMime.includes('text/csv') ||
		effectiveMime.includes('text/tab-separated-values') ||
		effectiveMime.includes('parquet') ||
		effectiveMime.includes('excel');
	$: isJson = effectiveMime.includes('application/json') || (meta?.payloadSchema as any)?.type === 'json';
	$: isMarkdown = effectiveMime.includes('text/markdown');
	$: isImage = effectiveMime.toLowerCase().startsWith('image/');
	$: isTiff = effectiveMime.toLowerCase().startsWith('image/tiff');
	$: isAudio = effectiveMime.toLowerCase().startsWith('audio/');
	$: isVideo = effectiveMime.toLowerCase().startsWith('video/');
	$: colCount = tableCols.length || ((meta?.payloadSchema as any)?.columns?.length ?? 0);
	$: builtinEnvironment = normalizeBuiltinEnvironment(meta);
	$: hasBuiltinEnvironment = Boolean(builtinEnvironment);
	$: builtinPackageCount = builtinEnvironment?.packages?.length ?? 0;

	function normalizeBuiltinEnvironment(
		artifactMeta: ArtifactMeta | null
	): { profileId: string; source: string; packages: string[] } | null {
		if (!artifactMeta) return null;
		const top = artifactMeta.builtinEnvironment;
		const fromSchema = (artifactMeta.payloadSchema as any)?.builtin_environment;
		const raw = (top && typeof top === 'object' ? top : fromSchema) as Record<string, unknown> | undefined;
		if (!raw || typeof raw !== 'object') return null;
		const profileId = String(raw.profileId ?? '').trim();
		const source = String(raw.source ?? '').trim();
		const packagesRaw = raw.packages;
		const packages: string[] = Array.isArray(packagesRaw)
			? packagesRaw
					.filter((p) => typeof p === 'string')
					.map((p) => String(p).trim())
					.filter((p) => p.length > 0)
			: [];
		if (!profileId && !source && packages.length === 0) return null;
		return { profileId, source, packages };
	}

	function clearImageUrl(): void {
		if (imageUrl) {
			URL.revokeObjectURL(imageUrl);
			imageUrl = null;
		}
	}

	function clearAudioUrl(): void {
		if (audioUrl) {
			URL.revokeObjectURL(audioUrl);
			audioUrl = null;
		}
	}

	function clearVideoUrl(): void {
		if (videoUrl) {
			URL.revokeObjectURL(videoUrl);
			videoUrl = null;
		}
	}

	onDestroy(() => {
		clearImageUrl();
		clearAudioUrl();
		clearVideoUrl();
	});

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
			await navigator.clipboard.writeText(activeArtifactId || artifactId);
		} catch {}
	}

	async function copyContentHash() {
		const hash = meta?.contentHash;
		if (!hash) return;
		try {
			await navigator.clipboard.writeText(hash);
		} catch {}
	}

	async function copyLineageJson() {
		if (!lineageJson) return;
		try {
			await navigator.clipboard.writeText(JSON.stringify(lineageJson, null, 2));
		} catch {}
	}

	async function copyArtifactLink() {
		const id = activeArtifactId || artifactId;
		if (!id) return;
		const url = `${window.location.origin}/artifacts/${encodeURIComponent(id)}`;
		try {
			await navigator.clipboard.writeText(url);
		} catch {}
	}

	function shortId(id: string): string {
		if (!id) return '';
		return id.length > 16 ? `${id.slice(0, 12)}...` : id;
	}

	function alphaInputLabel(idx: number): string {
		let n = idx + 1;
		let out = '';
		while (n > 0) {
			const rem = (n - 1) % 26;
			out = String.fromCharCode(65 + rem) + out;
			n = Math.floor((n - 1) / 26);
		}
		return `Input ${out}`;
	}

	function rowsFromSchema(ps: Record<string, any> | null | undefined): string | number {
		if (!ps || typeof ps !== 'object') return '-';
		return (ps as any).row_count ?? '-';
	}

	function colsFromSchema(ps: Record<string, any> | null | undefined): string | number {
		if (!ps || typeof ps !== 'object') return '-';
		const cols = (ps as any).columns;
		return Array.isArray(cols) ? cols.length : '-';
	}

	function ensureGraphId(): string {
		const g = String(graphId ?? '').trim();
		if (!g) throw new Error('graphId is required for artifact requests');
		return g;
	}

	async function loadInputSummaries(ids: string[], refs?: { artifactId: string; label?: string }[]) {
		inputArtifacts = [];
		if (!Array.isArray(ids) || ids.length === 0) return;
		const labelById = new Map<string, string>();
		if (Array.isArray(refs)) {
			for (const r of refs) {
				if (r?.artifactId) labelById.set(r.artifactId, String(r.label ?? '').trim());
			}
		}
		const summaries = await Promise.all(
			ids.map(async (id, idx) => {
				try {
					const res = await fetch(getArtifactMetaUrl(id, ensureGraphId()));
					if (!res.ok) return null;
					const m = await res.json();
					return {
						label: labelById.get(id) || alphaInputLabel(idx),
						artifactId: String(m.artifactId ?? id),
						nodeKind: String(m.nodeKind ?? '-'),
						mimeType: String(m.mimeType ?? '-'),
						rows: rowsFromSchema(m.payloadSchema),
						cols: colsFromSchema(m.payloadSchema)
					} as InputArtifactSummary;
				} catch {
					return null;
				}
			})
		);
		inputArtifacts = summaries.filter((x): x is InputArtifactSummary => Boolean(x));
	}

	async function openArtifact(id: string) {
		if (!id) return;
		activeArtifactId = id;
		void loadArtifact(id);
	}

	function jumpToProducer() {
		const nid = meta?.producerNodeId;
		if (!nid || !onJumpToNode) return;
		onJumpToNode(nid);
	}

	async function loadConsumers(id: string) {
		consumers = [];
		try {
			const res = await fetch(getArtifactConsumersUrl(id, ensureGraphId(), 50));
			if (!res.ok) return;
			const body = await res.json();
			consumers = Array.isArray(body?.consumers) ? body.consumers : [];
		} catch {}
	}

	async function loadLineage(id: string) {
		lineageJson = null;
		try {
			const res = await fetch(getArtifactLineageUrl(id, ensureGraphId(), 1));
			if (!res.ok) return;
			lineageJson = await res.json();
		} catch {}
	}

	function jumpToConsumerNode(nodeId: string) {
		if (!nodeId || !onJumpToNode) return;
		onJumpToNode(nodeId);
	}

	async function loadArtifact(id: string) {
		loading = true;
		error = null;
		text = null;
		jsonObj = null;
		clearImageUrl();
		clearAudioUrl();
		clearVideoUrl();
		tableRows = [];
		tableCols = [];
		totalRows = 0;
		offset = 0;
		inputArtifacts = [];
		consumers = [];
		lineageJson = null;

		try {
			const m = await fetch(getArtifactMetaUrl(id, ensureGraphId()));
			if (!m.ok) throw new Error(`${m.status} ${m.statusText}`);
			meta = await m.json();
			const inputIds = (meta?.inputArtifactIds ?? meta?.upstreamArtifactIds ?? []) as string[];
			await loadInputSummaries(inputIds, meta?.inputRefs);
			await Promise.all([loadConsumers(id), loadLineage(id)]);

			if (isTableLike(meta?.mimeType ?? mimeType ?? '', meta?.payloadSchema)) {
				await loadTablePage(0);
				return;
			}

			const res = await fetch(getArtifactUrl(id, ensureGraphId()));
			if (!res.ok) {
				const body = await res.text().catch(() => '');
				throw new Error(`${res.status} ${res.statusText} ${body}`);
			}

			const ct = res.headers.get('content-type') ?? meta?.mimeType ?? mimeType ?? '';
			const raw = await res.arrayBuffer();
			const loweredCt = String(ct).toLowerCase();
			if (loweredCt.startsWith('image/tiff')) {
				return;
			}
			if (loweredCt.startsWith('image/')) {
				const blob = new Blob([raw], { type: ct || 'application/octet-stream' });
				imageUrl = URL.createObjectURL(blob);
				return;
			}
			if (String(ct).toLowerCase().startsWith('audio/')) {
				const blob = new Blob([raw], { type: ct || 'application/octet-stream' });
				audioUrl = URL.createObjectURL(blob);
				return;
			}
			if (String(ct).toLowerCase().startsWith('video/')) {
				const blob = new Blob([raw], { type: ct || 'application/octet-stream' });
				videoUrl = URL.createObjectURL(blob);
				return;
			}
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
	$: componentWrapperOutputs = extractComponentWrapperOutputs(jsonObj);
	$: analysisArtifacts = extractAnalysisArtifacts(jsonObj);

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
				getArtifactPreviewUrl(activeArtifactId || artifactId, ensureGraphId(), nextOffset, limit)
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
			const res = await fetch(getArtifactUrl(activeArtifactId || artifactId, ensureGraphId()));
			if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
			const blob = await res.blob();
			const url = URL.createObjectURL(blob);
			const a = document.createElement('a');
			a.href = url;
			a.download = `${(activeArtifactId || artifactId).slice(0, 12)}`;
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
	<span class="chip"><b>type</b> {effectivePayloadType}</span>
	<span class="chip"><b>mime</b> {effectiveMime || '-'}</span>
	<span class="chip"><b>payload</b> {payloadSchemaType}</span>
	<span class="chip"><b>rows</b> {isTable ? totalRows || '-' : '-'}</span>
	<span class="chip"><b>cols</b> {isTable ? colCount || '-' : '-'}</span>
	{#if hasBuiltinEnvironment}
		<span class="chip chipBuiltin"><b>builtin</b> {builtinEnvironment?.profileId || 'custom'}</span>
	{/if}
	{#if cacheDecision}
		<span class="chip"><b>cache</b> {cacheDecision}</span>
	{:else if cached}
		<span class="chip"><b>cache</b> cache_hit</span>
	{/if}
	{#if meta?.contentHash}
		<span class="chip"><b>hash</b> {shortId(meta.contentHash)}</span>
	{/if}
	<button class="copyBtn" on:click={copyLineageJson}>Copy Lineage JSON</button>
	<button class="copyBtn" on:click={copyArtifactLink}>Copy Link</button>
	<button class="download" on:click={downloadArtifact}>Download</button>
</div>

<div class="meta">
	<div class="artifactRow">
		<b>artifactId:</b>
		<button class="copyBtn" on:click={copyArtifactId}>Copy</button>
		<span class="artifactId">{activeArtifactId || artifactId}</span>
	</div>
	{#if meta?.contentHash}
		<div class="artifactRow">
			<b>contentHash:</b>
			<button class="copyBtn" on:click={copyContentHash}>Copy</button>
			<span class="artifactId">{meta.contentHash}</span>
		</div>
	{/if}
	{#if meta?.producerNodeId || meta?.producerRunId}
		<div class="producerRow">
			<b>Produced by:</b>
			{#if meta?.producerNodeId}
				<button class="producerBtn" on:click={jumpToProducer}>node {meta.producerNodeId}</button>
			{:else}
				<span>node -</span>
			{/if}
			<span>run {meta?.producerRunId || '-'}</span>
		</div>
	{/if}
	{#if hasBuiltinEnvironment}
		<div class="inputsRow">
			<b>Builtin Environment:</b>
			<div class="builtinSummary">
				<span class="inputMeta">profile {builtinEnvironment?.profileId || '-'}</span>
				<span class="inputMeta">source {builtinEnvironment?.source || '-'}</span>
				<span class="inputMeta">{builtinPackageCount} package(s)</span>
			</div>
			<details class="builtinDetails">
				<summary>Packages</summary>
				<div class="builtinPackages">
					{#if builtinPackageCount > 0}
						{#each builtinEnvironment?.packages ?? [] as pkg}
							<span class="inputMeta">{pkg}</span>
						{/each}
					{:else}
						<span class="muted">No packages declared.</span>
					{/if}
				</div>
			</details>
		</div>
	{/if}
	{#if inputArtifacts.length > 0}
		<div class="inputsRow">
			<b>Inputs:</b>
			<div class="inputsList">
				{#each inputArtifacts as inp}
					<button class="inputItem" on:click={() => openArtifact(inp.artifactId)}>
						<span class="inputLabel">{inp.label}</span>
						<span class="inputId">{shortId(inp.artifactId)}</span>
						<span class="inputMeta">{inp.nodeKind}</span>
						<span class="inputMeta">{inp.mimeType}</span>
						<span class="inputMeta">rows {inp.rows}</span>
						<span class="inputMeta">cols {inp.cols}</span>
					</button>
				{/each}
			</div>
		</div>
	{/if}
	{#if consumers.length > 0}
		<div class="inputsRow">
			<b>Used by:</b>
			<div class="inputsList">
				{#each consumers as c}
					<div class="consumerItem">
						<span class="inputMeta">node {c.consumerNodeId}</span>
						<span class="inputMeta">run {c.consumerRunId}</span>
						<button class="copyBtn" on:click={() => jumpToConsumerNode(c.consumerNodeId)}>Jump</button>
						<button class="copyBtn" on:click={() => openArtifact(c.outputArtifactId)}>
							Open Output
						</button>
					</div>
				{/each}
			</div>
		</div>
	{/if}
	{#if lineageJson?.lineage}
		<details class="miniGraph" open>
			<summary>Lineage Graph</summary>
			<div class="miniGraphGrid">
				<div class="miniCol">
					<div class="miniTitle">Inputs</div>
					{#each inputArtifacts as inp}
						<button class="miniNode" on:click={() => openArtifact(inp.artifactId)}>
							{inp.label} {shortId(inp.artifactId)}
						</button>
					{/each}
				</div>
				<div class="miniCol">
					<div class="miniTitle">Producer</div>
					<button class="miniNode" on:click={jumpToProducer}>
						{meta?.producerNodeId ? `node ${meta.producerNodeId}` : 'node -'}
					</button>
					<div class="miniNode current">artifact {shortId(activeArtifactId || artifactId)}</div>
				</div>
				<div class="miniCol">
					<div class="miniTitle">Consumers</div>
					{#each consumers.slice(0, 8) as c}
						<button class="miniNode" on:click={() => jumpToConsumerNode(c.consumerNodeId)}>
							node {c.consumerNodeId}
						</button>
					{/each}
				</div>
			</div>
		</details>
	{/if}
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
	{#if componentWrapperOutputs.length > 0}
		<div class="block">
			<div class="label">Component Outputs</div>
			<div class="inputsList">
				{#each componentWrapperOutputs as outRef (`${outRef.name}:${outRef.artifactId}`)}
					<button class="inputItem" on:click={() => openArtifact(outRef.artifactId)}>
						<span class="inputLabel">{outRef.name}</span>
						<span class="inputId">{shortId(outRef.artifactId)}</span>
						<span class="inputMeta">{outRef.payloadType}</span>
						<span class="inputMeta">{outRef.mimeType}</span>
					</button>
				{/each}
			</div>
		</div>
	{/if}
	{#if analysisArtifacts.length > 0}
		<div class="block">
			<div class="label">Analysis Artifacts</div>
			<div class="inputsList">
				{#each analysisArtifacts as item (`${item.name}:${item.kind}`)}
					<div class="inputItem staticItem">
						<span class="inputLabel">{item.name}</span>
						<span class="inputMeta">{item.kind}</span>
						<span class="inputMeta">rows {item.rowCount}</span>
						<span class="inputMeta">fields {item.fields.length}</span>
						{#if item.description}
							<span class="inputMeta">{item.description}</span>
						{/if}
					</div>
				{/each}
			</div>
		</div>
	{/if}
	{#if hasPayloadSchema}
		<div class="block">
			<div class="label">Typed Schema</div>
			<div class="jsonTools">
				<button on:click={() => navigator.clipboard.writeText(JSON.stringify(meta?.payloadSchema ?? {}, null, 2))}>Copy</button>
			</div>
			<details open={true}>
				<summary>Schema</summary>
				<JsonTreeNode value={meta?.payloadSchema} />
			</details>
		</div>
	{/if}
{:else if text !== null}
	<div class="block">
		<div class="label">{isMarkdown ? 'Markdown' : 'Content'}</div>
		{#if isMarkdown}
			<div class="markdown">{@html renderSafeMarkdown(text)}</div>
		{:else}
			<pre>{text}</pre>
		{/if}
	</div>
	{#if hasPayloadSchema}
		<div class="block">
			<div class="label">Typed Schema</div>
			<div class="jsonTools">
				<button on:click={() => navigator.clipboard.writeText(JSON.stringify(meta?.payloadSchema ?? {}, null, 2))}>Copy</button>
			</div>
			<details>
				<summary>Schema</summary>
				<JsonTreeNode value={meta?.payloadSchema} />
			</details>
		</div>
	{/if}
{:else if isTiff}
	<div class="block">
		<div class="label">Image</div>
		<div class="muted">TIFF preview is not available inline. Download to inspect.</div>
		<button class="downloadTiff" on:click={downloadArtifact}>Download TIFF</button>
	</div>
{:else if isImage && imageUrl}
	<div class="block">
		<div class="label">Image</div>
		<img class="imagePreview" src={imageUrl} alt="artifact preview" />
	</div>
{:else if isAudio && audioUrl}
	<div class="block">
		<div class="label">Audio</div>
		<audio class="audioPreview" src={audioUrl} controls preload="metadata">
			Your browser does not support audio playback.
		</audio>
	</div>
{:else if isVideo && videoUrl}
	<div class="block">
		<div class="label">Video</div>
		<!-- svelte-ignore a11y_media_has_caption -->
		<video class="videoPreview" src={videoUrl} controls preload="metadata">
			Your browser does not support video playback.
		</video>
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
		opacity: 1;
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
	.chipBuiltin {
		border-color: #0ea5e9;
	}
	.download {
		margin-left: auto;
	}
	.download,
	.pager button,
	.pager select,
	.copyBtn,
	.producerBtn {
		background: var(--av-surface-alt);
		color: var(--av-text);
		border: 1px solid var(--av-border);
		border-radius: 6px;
	}
	.producerBtn {
		padding: 2px 8px;
		cursor: pointer;
	}
	.artifactRow {
		display: flex;
		align-items: center;
		gap: 8px;
		flex-wrap: wrap;
	}
	.producerRow {
		display: flex;
		align-items: center;
		gap: 8px;
		flex-wrap: wrap;
		margin-top: 6px;
	}
	.inputsRow {
		margin-top: 8px;
		display: grid;
		gap: 6px;
	}
	.inputsList {
		display: grid;
		gap: 6px;
	}
	.builtinSummary {
		display: flex;
		gap: 8px;
		flex-wrap: wrap;
	}
	.builtinDetails {
		border: 1px solid var(--av-border);
		border-radius: 8px;
		padding: 6px 8px;
		background: var(--av-surface-alt);
	}
	.builtinPackages {
		margin-top: 6px;
		display: flex;
		gap: 6px;
		flex-wrap: wrap;
	}
	.inputItem {
		display: flex;
		align-items: center;
		gap: 8px;
		flex-wrap: wrap;
		padding: 6px 8px;
		border: 1px solid var(--av-border);
		background: var(--av-surface-alt);
		color: var(--av-text);
		border-radius: 8px;
		text-align: left;
		cursor: pointer;
	}
	.staticItem {
		cursor: default;
	}
	.inputId {
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New',
			monospace;
	}
	.inputLabel {
		font-weight: 700;
		font-size: 11px;
	}
	.inputMeta {
		padding: 2px 6px;
		border-radius: 999px;
		border: 1px solid var(--av-border);
		background: var(--av-chip-bg);
		color: var(--av-chip-text);
		font-size: 11px;
	}
	.consumerItem {
		display: flex;
		align-items: center;
		flex-wrap: wrap;
		gap: 8px;
		padding: 6px 8px;
		border: 1px solid var(--av-border);
		border-radius: 8px;
		background: var(--av-surface-alt);
	}
	.miniGraph {
		margin-top: 10px;
	}
	.miniGraphGrid {
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		gap: 8px;
		margin-top: 8px;
	}
	.miniCol {
		border: 1px solid var(--av-border);
		border-radius: 8px;
		padding: 8px;
		background: var(--av-surface-alt);
		display: grid;
		gap: 6px;
	}
	.miniTitle {
		font-weight: 700;
		font-size: 11px;
		opacity: 0.9;
	}
	.miniNode {
		padding: 6px 8px;
		border: 1px solid var(--av-border);
		border-radius: 6px;
		background: var(--av-surface);
		color: var(--av-text);
		text-align: left;
		font-size: 11px;
	}
	.miniNode.current {
		background: var(--av-chip-bg);
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
	.imagePreview {
		display: block;
		max-width: 100%;
		max-height: 480px;
		border: 1px solid var(--av-border);
		border-radius: 8px;
		background: var(--av-surface-alt);
	}
	.audioPreview {
		display: block;
		width: 100%;
	}
	.videoPreview {
		display: block;
		width: 100%;
		max-height: 520px;
		border: 1px solid var(--av-border);
		border-radius: 8px;
		background: var(--av-surface-alt);
	}
	.downloadTiff {
		margin-top: 8px;
		background: var(--av-surface-alt);
		color: var(--av-text);
		border: 1px solid var(--av-border);
		border-radius: 6px;
		padding: 6px 10px;
	}
</style>
