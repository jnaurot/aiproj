<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { stringifyJson, tryParseJson } from '$lib/flow/components/editors/shared';

	type JsParams = Extract<ToolParams, { provider: 'js' }>;

	export let params: Partial<JsParams>;
	export let onDraft: (patch: Partial<JsParams>) => void;
	export let onCommit: (patch: Partial<JsParams>) => void;

	const defaultJs: JsParams['js'] = {
		code: '',
		args: {},
		capture_output: true
	};

	let argsDraft = '{}';
	let argsError: string | null = null;
	let lastArgsHydrationSignature = '';

	$: js = params?.js ?? defaultJs;
	$: captureOutput = Boolean(js.capture_output ?? true);
	$: argsHydrationSignature = JSON.stringify(js.args ?? {});
	$: if (argsHydrationSignature !== lastArgsHydrationSignature) {
		lastArgsHydrationSignature = argsHydrationSignature;
		argsDraft = stringifyJson(js.args ?? {}, '{}');
		argsError = null;
	}

	function validateArgsJson(text: string): { value?: Record<string, unknown>; error?: string } {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return { error: 'invalid JSON' };
		if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
			return { error: 'args must be a JSON object' };
		}
		return { value: parsed as Record<string, unknown> };
	}
</script>

<Section title="JavaScript">
	<Field label="code">
		<Input
			multiline={true}
			rows={10}
			value={js.code ?? ''}
			onInput={(event) => onDraft({ js: { ...js, code: (event.currentTarget as HTMLTextAreaElement).value } })}
			onBlur={(event) => onCommit({ js: { ...js, code: (event.currentTarget as HTMLTextAreaElement).value } })}
		/>
	</Field>

	<Field label="capture_output">
		<Input
			type="checkbox"
			checked={captureOutput}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ js: { ...js, capture_output: checked } });
				onCommit({ js: { ...js, capture_output: checked } });
			}}
		/>
	</Field>

	<Field label="args">
		<Input
			multiline={true}
			rows={6}
			value={argsDraft}
			onInput={(event) => {
				argsDraft = (event.currentTarget as HTMLTextAreaElement).value;
				argsError = validateArgsJson(argsDraft).error ?? null;
			}}
			onBlur={(event) => {
				argsDraft = (event.currentTarget as HTMLTextAreaElement).value;
				const validated = validateArgsJson(argsDraft);
				argsError = validated.error ?? null;
				if (!argsError && validated.value) onCommit({ js: { ...js, args: validated.value } });
			}}
		/>
		{#if argsError}
			<div class="fieldError">{argsError}</div>
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
