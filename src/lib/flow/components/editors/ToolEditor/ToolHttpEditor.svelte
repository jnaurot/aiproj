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

	$: http = params?.http ?? { url: 'https://', method: 'GET', headers: {}, query: {}, body: null };
	$: headersText = stringifyJson(http.headers ?? {}, '{}');
	$: queryText = stringifyJson(http.query ?? {}, '{}');
	$: bodyText = stringifyJson(http.body ?? null, 'null');

	function commitJson(key: 'headers' | 'query' | 'body', text: string): void {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return;
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

	<Field label="headers">
		<Input multiline={true} rows={4} value={headersText} onBlur={(event) => commitJson('headers', (event.currentTarget as HTMLTextAreaElement).value)} />
	</Field>

	<Field label="query">
		<Input multiline={true} rows={4} value={queryText} onBlur={(event) => commitJson('query', (event.currentTarget as HTMLTextAreaElement).value)} />
	</Field>

	<Field label="body">
		<Input multiline={true} rows={6} value={bodyText} onBlur={(event) => commitJson('body', (event.currentTarget as HTMLTextAreaElement).value)} />
	</Field>
</Section>
