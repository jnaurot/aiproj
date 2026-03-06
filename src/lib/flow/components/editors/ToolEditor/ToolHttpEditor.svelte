<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { stringifyJson, tryParseJson } from '$lib/flow/components/editors/shared';

	type HttpParams = Extract<ToolParams, { provider: 'http' }>;
	type Method = HttpParams['http']['method'];

	export let params: Partial<HttpParams>;
	export let onDraft: (patch: Partial<HttpParams>) => void;
	export let onCommit: (patch: Partial<HttpParams>) => void;

	const defaultHttp: HttpParams['http'] = {
		url: 'https://',
		method: 'GET',
		headers: {},
		query: {},
		body: null
	};

	let headersDraft = '{}';
	let queryDraft = '{}';
	let bodyDraft = 'null';
	let headersError: string | null = null;
	let queryError: string | null = null;
	let bodyError: string | null = null;
	let lastHydrationSignature = '';

	$: http = params?.http ?? defaultHttp;
	$: cacheEnabled = Boolean(params?.cache_enabled ?? true);
	$: hydrationSignature = JSON.stringify({
		headers: http.headers ?? {},
		query: http.query ?? {},
		body: http.body ?? null
	});
	$: if (hydrationSignature !== lastHydrationSignature) {
		lastHydrationSignature = hydrationSignature;
		headersDraft = stringifyJson(http.headers ?? {}, '{}');
		queryDraft = stringifyJson(http.query ?? {}, '{}');
		bodyDraft = stringifyJson(http.body ?? null, 'null');
		headersError = null;
		queryError = null;
		bodyError = null;
	}

	function isPlainObject(value: unknown): value is Record<string, unknown> {
		return typeof value === 'object' && value !== null && !Array.isArray(value);
	}

	function validateHeaders(value: unknown): string | null {
		if (!isPlainObject(value)) return 'headers must be a JSON object';
		for (const [key, childValue] of Object.entries(value)) {
			if (typeof childValue !== 'string') return `headers.${key} must be a string`;
		}
		return null;
	}

	function validateQuery(value: unknown): string | null {
		if (!isPlainObject(value)) return 'query must be a JSON object';
		for (const [key, childValue] of Object.entries(value)) {
			if (!(typeof childValue === 'string' || typeof childValue === 'number' || typeof childValue === 'boolean')) {
				return `query.${key} must be string, number, or boolean`;
			}
		}
		return null;
	}

	function validateDraft(key: 'headers' | 'query' | 'body', text: string): string | null {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return 'invalid JSON';
		if (key === 'headers') return validateHeaders(parsed);
		if (key === 'query') return validateQuery(parsed);
		return null;
	}

	function commitJson(key: 'headers' | 'query' | 'body', text: string): void {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return;

		if (key === 'headers') {
			const validationError = validateHeaders(parsed);
			if (validationError) return;
		}
		if (key === 'query') {
			const validationError = validateQuery(parsed);
			if (validationError) return;
		}

		onCommit({ http: { ...http, [key]: parsed } });
	}
</script>

<Section title="HTTP">
	<Field label="url">
		<Input
			value={http.url ?? ''}
			onInput={(event) => onDraft({ http: { ...http, url: (event.currentTarget as HTMLInputElement).value } })}
			onBlur={(event) => onCommit({ http: { ...http, url: (event.currentTarget as HTMLInputElement).value } })}
		/>
	</Field>

	<Field label="method">
		<select
			value={http.method ?? 'GET'}
			on:change={(event) => {
				const value = (event.currentTarget as HTMLSelectElement).value as Method;
				onDraft({ http: { ...http, method: value } });
				onCommit({ http: { ...http, method: value } });
			}}
		>
			<option value="GET">GET</option>
			<option value="POST">POST</option>
			<option value="PUT">PUT</option>
			<option value="PATCH">PATCH</option>
			<option value="DELETE">DELETE</option>
		</select>
	</Field>

	<Field label="cached">
		<Input
			type="checkbox"
			checked={cacheEnabled}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ cache_enabled: checked });
				onCommit({ cache_enabled: checked });
			}}
		/>
	</Field>

	<Field label="headers">
		<Input
			multiline={true}
			rows={4}
			value={headersDraft}
			onInput={(event) => {
				headersDraft = (event.currentTarget as HTMLTextAreaElement).value;
				headersError = validateDraft('headers', headersDraft);
			}}
			onBlur={(event) => {
				headersDraft = (event.currentTarget as HTMLTextAreaElement).value;
				headersError = validateDraft('headers', headersDraft);
				if (!headersError) commitJson('headers', headersDraft);
			}}
		/>
		{#if headersError}
			<div class="fieldError">{headersError}</div>
		{/if}
	</Field>

	<Field label="query">
		<Input
			multiline={true}
			rows={4}
			value={queryDraft}
			onInput={(event) => {
				queryDraft = (event.currentTarget as HTMLTextAreaElement).value;
				queryError = validateDraft('query', queryDraft);
			}}
			onBlur={(event) => {
				queryDraft = (event.currentTarget as HTMLTextAreaElement).value;
				queryError = validateDraft('query', queryDraft);
				if (!queryError) commitJson('query', queryDraft);
			}}
		/>
		{#if queryError}
			<div class="fieldError">{queryError}</div>
		{/if}
	</Field>

	<Field label="body">
		<Input
			multiline={true}
			rows={6}
			value={bodyDraft}
			onInput={(event) => {
				bodyDraft = (event.currentTarget as HTMLTextAreaElement).value;
				bodyError = validateDraft('body', bodyDraft);
			}}
			onBlur={(event) => {
				bodyDraft = (event.currentTarget as HTMLTextAreaElement).value;
				bodyError = validateDraft('body', bodyDraft);
				if (!bodyError) commitJson('body', bodyDraft);
			}}
		/>
		{#if bodyError}
			<div class="fieldError">{bodyError}</div>
		{/if}
	</Field>
</Section>

<style>
	.fieldError {
		margin-top: 6px;
		font-size: 12px;
		color: #f87171;
	}
</style>
