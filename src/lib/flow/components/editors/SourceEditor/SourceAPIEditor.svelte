<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceAPIParams, SourceOutputMode } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import KeyValueEditor from '$lib/flow/components/KeyValueEditor.svelte';
	import { asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	type Method = SourceAPIParams['method'];
	type AuthType = SourceAPIParams['auth_type'];
	type SourceAPIPatch = Partial<SourceAPIParams>;

	export let selectedNode: Node<PipelineNodeData> | null;
	export let params: Partial<SourceAPIParams>;
	export let onDraft: (patch: SourceAPIPatch) => void;
	export let onCommit: (patch: SourceAPIPatch) => void;

	const methods: Method[] = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'];
	const authTypes: AuthType[] = ['none', 'bearer', 'basic', 'api_key'];
	const outputModes: SourceOutputMode[] = ['table', 'text', 'json', 'binary'];
	const cacheModes = ['default', 'never', 'ttl'] as const;

	$: url = asString(params?.url, '');
	$: method = (asString(params?.method, 'GET') as Method) ?? 'GET';
	$: auth_type = (asString(params?.auth_type, 'none') as AuthType) ?? 'none';
	$: auth_token_ref = asString(params?.auth_token_ref, '');
	$: timeout_seconds = asNumberOrEmpty(params?.timeout_seconds ?? 30);
	$: headers = params?.headers ?? {};
	$: body = params?.body ?? {};
	$: outputMode = (asString(params?.output?.mode, 'json') as SourceOutputMode) ?? 'json';
	$: cacheMode = asString(params?.cache_policy?.mode, 'default');
	$: cacheTtl = asNumberOrEmpty(params?.cache_policy?.ttl_seconds);

	let headersDraft: Record<string, string> = headers;
	let bodyDraft: Record<string, unknown> = body as Record<string, unknown>;

	$: if (JSON.stringify(headersDraft) !== JSON.stringify(headers)) {
		headersDraft = { ...headers };
	}
	$: if (JSON.stringify(bodyDraft) !== JSON.stringify(body)) {
		bodyDraft = { ...(body as Record<string, unknown>) };
	}

	function draft(patch: SourceAPIPatch): void {
		onDraft?.(patch);
	}

	function commit(patch: SourceAPIPatch): void {
		onCommit?.(patch);
	}
</script>

{#if selectedNode}
	<Section title="API">
		<Field label="url">
			<Input
				value={url}
				placeholder="https://api.example.com/data"
				onInput={(event) => draft({ url: (event.currentTarget as HTMLInputElement).value })}
				onBlur={(event) => commit({ url: (event.currentTarget as HTMLInputElement).value })}
			/>
		</Field>

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

		<Field label="auth_token_ref">
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

		<Field label="headers">
			<KeyValueEditor
				label=""
				value={headersDraft}
				allowTypes={false}
				onChange={(next) => {
					headersDraft = next as Record<string, string>;
					draft({ headers: headersDraft });
				}}
			/>
			<button
				type="button"
				on:click={() => commit({ headers: headersDraft })}
			>
				Apply headers
			</button>
		</Field>

		<Field label="body">
			<KeyValueEditor
				label=""
				value={bodyDraft}
				allowTypes={true}
				defaultType="string"
				onChange={(next) => {
					const keys = Object.keys(next ?? {});
					bodyDraft = next as Record<string, unknown>;
					draft({ body: keys.length > 0 ? (bodyDraft as Record<string, unknown>) : undefined });
				}}
			/>
			<button
				type="button"
				on:click={() => {
					const keys = Object.keys(bodyDraft ?? {});
					commit({ body: keys.length > 0 ? bodyDraft : undefined });
				}}
			>
				Apply body
			</button>
		</Field>

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
							ttl_seconds:
								mode === 'ttl'
									? (params?.cache_policy?.ttl_seconds ?? 60)
									: undefined
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
	</Section>
{/if}
