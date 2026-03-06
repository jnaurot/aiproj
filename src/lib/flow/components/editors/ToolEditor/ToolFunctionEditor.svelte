<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { stringifyJson, tryParseJson } from '$lib/flow/components/editors/shared';

	type FunctionParams = Extract<ToolParams, { provider: 'function' }>;

	export let params: Partial<FunctionParams>;
	export let onDraft: (patch: Partial<FunctionParams>) => void;
	export let onCommit: (patch: Partial<FunctionParams>) => void;

	const defaultFunction: FunctionParams['function'] = {
		module: '',
		export: '',
		args: {},
		capture_output: true
	};

	let argsDraft = '{}';
	let argsError: string | null = null;
	let lastArgsHydrationSignature = '';

	$: fn = params?.function ?? defaultFunction;
	$: captureOutput = Boolean(fn.capture_output ?? true);
	$: argsHydrationSignature = JSON.stringify(fn.args ?? {});
	$: if (argsHydrationSignature !== lastArgsHydrationSignature) {
		lastArgsHydrationSignature = argsHydrationSignature;
		argsDraft = stringifyJson(fn.args ?? {}, '{}');
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

<Section title="Function">
	<Field label="module">
		<Input
			value={fn.module ?? ''}
			onInput={(event) => onDraft({ function: { ...fn, module: (event.currentTarget as HTMLInputElement).value } })}
			onBlur={(event) => onCommit({ function: { ...fn, module: (event.currentTarget as HTMLInputElement).value } })}
		/>
	</Field>

	<Field label="export">
		<Input
			value={fn.export ?? ''}
			onInput={(event) => onDraft({ function: { ...fn, export: (event.currentTarget as HTMLInputElement).value } })}
			onBlur={(event) => onCommit({ function: { ...fn, export: (event.currentTarget as HTMLInputElement).value } })}
		/>
	</Field>

	<Field label="capture_output">
		<Input
			type="checkbox"
			checked={captureOutput}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ function: { ...fn, capture_output: checked } });
				onCommit({ function: { ...fn, capture_output: checked } });
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
				if (!argsError && validated.value) onCommit({ function: { ...fn, args: validated.value } });
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
