<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { stringifyJson, tryParseJson } from '$lib/flow/components/editors/shared';

	type PythonParams = Extract<ToolParams, { provider: 'python' }>;

	export let params: Partial<PythonParams>;
	export let onDraft: (patch: Partial<PythonParams>) => void;
	export let onCommit: (patch: Partial<PythonParams>) => void;

	const defaultPython: PythonParams['python'] = {
		code: '',
		args: {},
		capture_output: true
	};

	let argsDraft = '{}';
	let argsError: string | null = null;
	let lastArgsHydrationSignature = '';

	$: python = params?.python ?? defaultPython;
	$: captureOutput = Boolean(python.capture_output ?? true);
	$: argsHydrationSignature = JSON.stringify(python.args ?? {});
	$: if (argsHydrationSignature !== lastArgsHydrationSignature) {
		lastArgsHydrationSignature = argsHydrationSignature;
		argsDraft = stringifyJson(python.args ?? {}, '{}');
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

<Section title="Python">
	<Field label="code">
		<Input
			multiline={true}
			rows={10}
			value={python.code ?? ''}
			onInput={(event) => onDraft({ python: { ...python, code: (event.currentTarget as HTMLTextAreaElement).value } })}
			onBlur={(event) => onCommit({ python: { ...python, code: (event.currentTarget as HTMLTextAreaElement).value } })}
		/>
	</Field>

	<Field label="capture_output">
		<Input
			type="checkbox"
			checked={captureOutput}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ python: { ...python, capture_output: checked } });
				onCommit({ python: { ...python, capture_output: checked } });
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
				if (!argsError && validated.value) onCommit({ python: { ...python, args: validated.value } });
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
