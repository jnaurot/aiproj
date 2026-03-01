<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceAPIParams, SourceOutputMode } from '$lib/flow/schema/source';
	import { graphStore } from '$lib/flow/store/graphStore';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Disclosure from '$lib/flow/components/ui/Disclosure.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import KeyValueEditor from '$lib/flow/components/KeyValueEditor.svelte';
	import { asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	type Method = SourceAPIParams['method'];
	type AuthType = SourceAPIParams['auth_type'];
	type BodyMode = SourceAPIParams['bodyMode'];
	type ContentType = NonNullable<SourceAPIParams['contentType']>;
	type SourceAPIPatch = Partial<SourceAPIParams>;

	export let selectedNode: Node<PipelineNodeData> | null;
	export let params: Partial<SourceAPIParams>;
	export let onDraft: (patch: SourceAPIPatch) => void;
	export let onCommit: (patch: SourceAPIPatch) => void;

	const methods: Method[] = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD'];
	const authTypes: AuthType[] = ['none', 'bearer', 'basic', 'api_key'];
	const outputModes: SourceOutputMode[] = ['table', 'text', 'json', 'binary'];
	const bodyModes: BodyMode[] = ['none', 'json', 'form', 'multipart', 'raw'];
	const cacheModes = ['default', 'never', 'ttl'] as const;
	const contentTypeOptions: Array<{ value: ContentType | 'none'; label: string }> = [
		{ value: 'application/json', label: 'application/json' },
		{ value: 'application/x-www-form-urlencoded', label: 'application/x-www-form-urlencoded' },
		{ value: 'multipart/form-data', label: 'multipart/form-data' },
		{ value: 'text/plain', label: 'text/plain' },
		{ value: 'application/xml', label: 'application/xml' },
		{ value: 'none', label: 'none' }
	];

	const contentTypeToMode: Record<ContentType, BodyMode> = {
		'application/json': 'json',
		'application/x-www-form-urlencoded': 'form',
		'multipart/form-data': 'multipart',
		'text/plain': 'raw',
		'application/xml': 'raw'
	};

	$: _uiTick = $graphStore.inspector.uiByNodeId;
	$: ui = selectedNode
		? graphStore.getInspectorUi(selectedNode.id, params as Record<string, any>)
		: {
				requestOpen: true,
				authOpen: false,
				transportOpen: false,
				executionOpen: false,
				debugOpen: false,
				queryOpen: false,
				headersOpen: false,
				bodyOpen: false
			};

	$: url = asString(params?.url, '');
	$: method = (asString(params?.method, 'GET') as Method) ?? 'GET';
	$: auth_type = (asString(params?.auth_type, 'none') as AuthType) ?? 'none';
	$: auth_token_ref = asString(params?.auth_token_ref, '');
	$: timeout_seconds = asNumberOrEmpty(params?.timeout_seconds ?? 30);
	$: headers = params?.headers ?? {};
	$: query = params?.query ?? {};
	$: bodyMode = (asString(params?.bodyMode, 'none') as BodyMode) ?? 'none';
	$: bodyJson = (params?.bodyJson ?? {}) as Record<string, unknown>;
	$: bodyForm = (params?.bodyForm ?? {}) as Record<string, string>;
	$: bodyRaw = asString(params?.bodyRaw, '');
	$: contentTypeValue = (params?.contentType as ContentType | undefined) ?? undefined;
	$: selectedContentType = contentTypeValue ?? 'none';
	$: outputMode = (asString(params?.output?.mode, 'json') as SourceOutputMode) ?? 'json';
	$: cacheMode = asString(params?.cache_policy?.mode, 'default');
	$: cacheTtl = asNumberOrEmpty(params?.cache_policy?.ttl_seconds);

	let headersDraft: Record<string, string> = headers;
	let queryDraft: Record<string, string> = query;
	let bodyJsonDraft: Record<string, unknown> = bodyJson;
	let bodyFormDraft: Record<string, string> = bodyForm;
	let bodyRawDraft = bodyRaw;

	$: if (JSON.stringify(headersDraft) !== JSON.stringify(headers)) headersDraft = { ...headers };
	$: if (JSON.stringify(queryDraft) !== JSON.stringify(query)) queryDraft = { ...query };
	$: if (JSON.stringify(bodyJsonDraft) !== JSON.stringify(bodyJson)) bodyJsonDraft = { ...bodyJson };
	$: if (JSON.stringify(bodyFormDraft) !== JSON.stringify(bodyForm)) bodyFormDraft = { ...bodyForm };
	$: if (bodyRawDraft !== bodyRaw) bodyRawDraft = bodyRaw;

	$: requestSummary = method;
	$: authSummary = auth_type;
	$: transportSummary = `timeout ${Number(timeout_seconds || 30)}s`;
	$: advancedSummary = `output ${outputMode} • cache ${cacheMode}`;
	$: querySummary = Object.keys(queryDraft ?? {}).length === 0 ? 'none' : `${Object.keys(queryDraft).length} params`;
	$: headersSummary = Object.keys(headersDraft ?? {}).length === 0 ? 'none' : `${Object.keys(headersDraft).length} headers`;
	$: bodySummary =
		bodyMode === 'none'
			? 'none'
			: bodyMode === 'json'
				? `json (${Object.keys(bodyJsonDraft ?? {}).length} keys)`
				: bodyMode === 'form' || bodyMode === 'multipart'
					? `${bodyMode} (${Object.keys(bodyFormDraft ?? {}).length} fields)`
					: `raw (${(bodyRawDraft ?? '').length} chars)`;

	function setUi(patch: Record<string, boolean>): void {
		if (!selectedNode) return;
		graphStore.setInspectorUi(selectedNode.id, patch);
	}

	function draft(patch: SourceAPIPatch): void {
		onDraft?.(patch);
	}

	function commit(patch: SourceAPIPatch): void {
		onCommit?.(patch);
	}

	function ensureNoContentTypeHeader(input: Record<string, string>): Record<string, string> {
		const next = { ...input };
		for (const key of Object.keys(next)) if (key.toLowerCase() === 'content-type') delete next[key];
		return next;
	}

	function setContentType(nextType: ContentType | 'none'): void {
		const nextHeaders = ensureNoContentTypeHeader(headersDraft);
		if (nextType === 'none') {
			const patch: SourceAPIPatch = {
				headers: nextHeaders,
				contentType: undefined,
				__managedHeaders: { ...(params?.__managedHeaders ?? {}), contentType: true }
			};
			headersDraft = nextHeaders;
			draft(patch);
			commit(patch);
			return;
		}
		nextHeaders['Content-Type'] = nextType;
		const impliedMode = contentTypeToMode[nextType] ?? bodyMode;
		const patch = buildBodyPatch(impliedMode, {
			headers: nextHeaders,
			contentType: nextType,
			__managedHeaders: { ...(params?.__managedHeaders ?? {}), contentType: true }
		});
		headersDraft = nextHeaders;
		draft(patch);
		commit(patch);
	}

	function buildBodyPatch(mode: BodyMode, extras: SourceAPIPatch = {}): SourceAPIPatch {
		if (mode === 'json') {
			return { ...extras, bodyMode: mode, bodyJson: { ...bodyJsonDraft }, bodyForm: undefined, bodyRaw: undefined };
		}
		if (mode === 'form' || mode === 'multipart') {
			return { ...extras, bodyMode: mode, bodyForm: { ...bodyFormDraft }, bodyJson: undefined, bodyRaw: undefined };
		}
		if (mode === 'raw') {
			return { ...extras, bodyMode: mode, bodyRaw: bodyRawDraft, bodyJson: undefined, bodyForm: undefined };
		}
		return { ...extras, bodyMode: 'none', bodyJson: undefined, bodyForm: undefined, bodyRaw: undefined };
	}

	function onHeaderDraft(next: Record<string, unknown>): void {
		const nextHeaders = Object.fromEntries(Object.entries(next ?? {}).map(([k, v]) => [k, String(v ?? '')]));
		headersDraft = nextHeaders;
		const current = Object.entries(nextHeaders).find(([k]) => k.toLowerCase() === 'content-type');
		const managed = current ? String(current[1] ?? '') === String(contentTypeValue ?? '') : !contentTypeValue;
		draft({ headers: nextHeaders, __managedHeaders: { ...(params?.__managedHeaders ?? {}), contentType: managed } });
	}

	function applyHeaders(): void {
		const current = Object.entries(headersDraft).find(([k]) => k.toLowerCase() === 'content-type');
		const managed = current ? String(current[1] ?? '') === String(contentTypeValue ?? '') : !contentTypeValue;
		commit({ headers: { ...headersDraft }, __managedHeaders: { ...(params?.__managedHeaders ?? {}), contentType: managed } });
	}

	function normalizeStringMap(next: Record<string, unknown>): Record<string, string> {
		return Object.fromEntries(Object.entries(next ?? {}).map(([k, v]) => [k, String(v ?? '')]));
	}

	function makeEffectiveUrl(rawUrl: string, q: Record<string, string>): string {
		const base = rawUrl || '';
		if (!base.trim()) return '';
		try {
			const u = new URL(base);
			for (const [k, v] of Object.entries(q)) u.searchParams.set(k, v);
			return u.toString();
		} catch {
			return base;
		}
	}

	$: effectiveUrl = makeEffectiveUrl(url, queryDraft);
	$: effectiveHeadersPreview = (() => {
		const next = { ...headersDraft };
		if (contentTypeValue) next['Content-Type'] = contentTypeValue;
		return Object.entries(next)
			.sort(([a], [b]) => a.localeCompare(b))
			.map(([k, v]) => `${k}: ${v}`);
	})();
</script>

{#if selectedNode}
	<Section title="API">
		<Disclosure
			title="Request"
			variant="primary"
			open={ui.requestOpen}
			onToggle={(open) => setUi({ requestOpen: open })}
			summaryRight={requestSummary}
		>
			<Field label="method">
				<select
					value={method}
					on:change={(event) => {
						const value = (event.currentTarget as HTMLSelectElement).value as Method;
						draft({ method: value });
						commit({ method: value });
					}}
				>
					{#each methods as option}
						<option value={option}>{option}</option>
					{/each}
				</select>
			</Field>

			<Field label="url" stacked={true}>
				<Input
					value={url}
					placeholder="https://api.example.com/data"
					onInput={(event) => draft({ url: (event.currentTarget as HTMLInputElement).value })}
					onBlur={(event) => commit({ url: (event.currentTarget as HTMLInputElement).value })}
				/>
			</Field>

			<Field label="Content-Type">
				<select
					value={selectedContentType}
					on:change={(event) =>
						setContentType((event.currentTarget as HTMLSelectElement).value as ContentType | 'none')}
				>
					{#each contentTypeOptions as option}
						<option value={option.value}>{option.label}</option>
					{/each}
				</select>
			</Field>

			<div class="hint">Selecting a Content-Type will write <code>Content-Type</code> into headers on Apply.</div>

			<Disclosure
				title="Query params"
				variant="sub"
				open={ui.queryOpen}
				onToggle={(open) => setUi({ queryOpen: open })}
				summaryRight={querySummary}
			>
				<KeyValueEditor
					label=""
					value={queryDraft}
					allowTypes={false}
					stacked={true}
					onChange={(next) => {
						const value = normalizeStringMap(next);
						queryDraft = value;
						draft({ query: value });
					}}
				/>
				<button type="button" on:click={() => commit({ query: { ...queryDraft } })}>Apply query params</button>
			</Disclosure>

			<Disclosure
				title="Headers"
				variant="sub"
				open={ui.headersOpen}
				onToggle={(open) => setUi({ headersOpen: open })}
				summaryRight={headersSummary}
			>
				<KeyValueEditor
					label=""
					value={headersDraft}
					allowTypes={false}
					stacked={true}
					onChange={(next) => onHeaderDraft(next as Record<string, unknown>)}
				/>
				<button type="button" on:click={applyHeaders}>Apply headers</button>
			</Disclosure>

			<Disclosure
				title="Body"
				variant="sub"
				open={ui.bodyOpen}
				onToggle={(open) => setUi({ bodyOpen: open })}
				summaryRight={bodySummary}
			>
				<Field label="body mode">
					<select
						value={bodyMode}
						on:change={(event) => {
							const mode = (event.currentTarget as HTMLSelectElement).value as BodyMode;
							const patch = buildBodyPatch(mode);
							draft(patch);
							commit(patch);
						}}
					>
						{#each bodyModes as mode}
							<option value={mode}>{mode}</option>
						{/each}
					</select>
				</Field>

				{#if bodyMode === 'none'}
					<div class="muted">No body</div>
				{:else if bodyMode === 'json'}
					<KeyValueEditor
						label=""
						value={bodyJsonDraft}
						allowTypes={true}
						defaultType="string"
						stacked={true}
						onChange={(next) => {
							bodyJsonDraft = { ...(next as Record<string, unknown>) };
							draft(buildBodyPatch('json'));
						}}
					/>
					<button type="button" on:click={() => commit(buildBodyPatch('json'))}>Apply JSON body</button>
				{:else if bodyMode === 'form'}
					<KeyValueEditor
						label=""
						value={bodyFormDraft}
						allowTypes={false}
						stacked={true}
						onChange={(next) => {
							bodyFormDraft = normalizeStringMap(next as Record<string, unknown>);
							draft(buildBodyPatch('form'));
						}}
					/>
					<button type="button" on:click={() => commit(buildBodyPatch('form'))}>Apply form fields</button>
				{:else if bodyMode === 'multipart'}
					<KeyValueEditor
						label=""
						value={bodyFormDraft}
						allowTypes={false}
						stacked={true}
						onChange={(next) => {
							bodyFormDraft = normalizeStringMap(next as Record<string, unknown>);
							draft(buildBodyPatch('multipart'));
						}}
					/>
					<button type="button" on:click={() => commit(buildBodyPatch('multipart'))}>Apply multipart fields</button>
				{:else}
					<Input
						multiline={true}
						rows={6}
						value={bodyRawDraft}
						onInput={(event) => {
							bodyRawDraft = (event.currentTarget as HTMLTextAreaElement).value;
							draft(buildBodyPatch('raw'));
						}}
					/>
					<button type="button" on:click={() => commit(buildBodyPatch('raw'))}>Apply raw body</button>
				{/if}
			</Disclosure>
		</Disclosure>

		<Disclosure
			title="Auth"
			variant="primary"
			open={ui.authOpen}
			onToggle={(open) => setUi({ authOpen: open })}
			summaryRight={authSummary}
		>
			<Field label="auth_type">
				<select
					value={auth_type}
					on:change={(event) => {
						const value = (event.currentTarget as HTMLSelectElement).value as AuthType;
						draft({ auth_type: value });
						commit({ auth_type: value });
					}}
				>
					{#each authTypes as option}
						<option value={option}>{option}</option>
					{/each}
				</select>
			</Field>

			<Field label="auth_token_ref" stacked={true}>
				<Input
					value={auth_token_ref}
					placeholder="ENV_VAR_NAME (required if auth_type != none)"
					onInput={(event) => {
						const value = (event.currentTarget as HTMLInputElement).value.trim();
						draft({ auth_token_ref: value === '' ? undefined : value });
					}}
					onBlur={(event) => {
						const value = (event.currentTarget as HTMLInputElement).value.trim();
						commit({ auth_token_ref: value === '' ? undefined : value });
					}}
				/>
			</Field>
		</Disclosure>

		<Disclosure
			title="Transport"
			variant="primary"
			open={ui.transportOpen}
			onToggle={(open) => setUi({ transportOpen: open })}
			summaryRight={transportSummary}
		>
			<Field label="timeout_seconds">
				<Input
					type="number"
					min="1"
					step="1"
					value={timeout_seconds}
					onInput={(event) =>
						draft({ timeout_seconds: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) ?? 30 })}
					onBlur={(event) =>
						commit({ timeout_seconds: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) ?? 30 })}
				/>
			</Field>
		</Disclosure>

		<Disclosure
			title="Advanced"
			variant="primary"
			open={ui.executionOpen}
			onToggle={(open) => setUi({ executionOpen: open })}
			summaryRight={advancedSummary}
		>
			<Field label="output mode">
				<select
					value={outputMode}
					on:change={(event) => {
						const mode = (event.currentTarget as HTMLSelectElement).value as SourceOutputMode;
						draft({ output: { ...(params?.output ?? {}), mode } });
						commit({ output: { ...(params?.output ?? {}), mode } });
					}}
				>
					{#each outputModes as mode}
						<option value={mode}>{mode}</option>
					{/each}
				</select>
			</Field>

			<Field label="cache_policy.mode">
				<select
					value={cacheMode}
					on:change={(event) => {
						const mode = (event.currentTarget as HTMLSelectElement).value as (typeof cacheModes)[number];
						const patch = {
							cache_policy: {
								...(params?.cache_policy ?? {}),
								mode,
								ttl_seconds: mode === 'ttl' ? (params?.cache_policy?.ttl_seconds ?? 60) : undefined
							}
						};
						draft(patch);
						commit(patch);
					}}
				>
					{#each cacheModes as mode}
						<option value={mode}>{mode}</option>
					{/each}
				</select>
			</Field>

			{#if cacheMode === 'ttl'}
				<Field label="cache_policy.ttl_seconds">
					<Input
						type="number"
						min="1"
						step="1"
						value={cacheTtl}
						onInput={(event) =>
							draft({
								cache_policy: {
									...(params?.cache_policy ?? { mode: 'ttl' }),
									mode: 'ttl',
									ttl_seconds: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1)
								}
							})}
						onBlur={(event) =>
							commit({
								cache_policy: {
									...(params?.cache_policy ?? { mode: 'ttl' }),
									mode: 'ttl',
									ttl_seconds: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1)
								}
							})}
					/>
				</Field>
			{/if}
		</Disclosure>

		<Disclosure
			title="Debug"
			variant="primary"
			open={ui.debugOpen}
			onToggle={(open) => setUi({ debugOpen: open })}
			summaryRight="effective request"
		>
			<div class="previewMono">
				<div><span class="muted">url:</span> {effectiveUrl || '-'}</div>
				<div><span class="muted">headers:</span> {effectiveHeadersPreview.length}</div>
				{#each effectiveHeadersPreview as line}
					<div>{line}</div>
				{/each}
				<div><span class="muted">body mode:</span> {bodyMode}</div>
			</div>
		</Disclosure>
	</Section>
{/if}

<style>
	.hint {
		font-size: 11px;
		opacity: 0.8;
		margin-top: -4px;
	}

	.hint code {
		font-family: ui-monospace, Menlo, Monaco, Consolas, 'Courier New', monospace;
	}

	.muted {
		opacity: 0.75;
	}

	.previewMono {
		font-family: ui-monospace, Menlo, Monaco, Consolas, 'Courier New', monospace;
		font-size: 12px;
		display: flex;
		flex-direction: column;
		gap: 4px;
		padding: 8px;
		border: 1px solid #253149;
		border-radius: 8px;
		background: rgba(11, 18, 32, 0.6);
		white-space: pre-wrap;
		word-break: break-word;
	}
</style>
