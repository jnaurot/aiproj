<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceAPIParams } from '$lib/flow/schema/source';
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

	$: url = asString(params?.url, '');
	$: method = (asString(params?.method, 'GET') as Method) ?? 'GET';
	$: auth_type = (asString(params?.auth_type, 'none') as AuthType) ?? 'none';
	$: auth_token_ref = asString(params?.auth_token_ref, '');
	$: timeout_seconds = asNumberOrEmpty(params?.timeout_seconds ?? 30);
	$: headers = params?.headers ?? {};
	$: body = params?.body ?? {};

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
			/>
		</Field>

		<Field label="headers">
			<KeyValueEditor
				label=""
				value={headers}
				allowTypes={false}
				onChange={(next) => draft({ headers: next as Record<string, string> })}
			/>
		</Field>

		<Field label="body">
			<KeyValueEditor
				label=""
				value={body as Record<string, unknown>}
				allowTypes={true}
				defaultType="string"
				onChange={(next) => {
					const keys = Object.keys(next ?? {});
					draft({ body: keys.length > 0 ? (next as Record<string, unknown>) : undefined });
				}}
			/>
		</Field>
	</Section>
{/if}
