<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { stringifyJson, tryParseJson } from '$lib/flow/components/editors/shared';

	type ShellParams = Extract<ToolParams, { provider: 'shell' }>;

	export let params: Partial<ShellParams>;
	export let onDraft: (patch: Partial<ShellParams>) => void;
	export let onCommit: (patch: Partial<ShellParams>) => void;

	const defaultShell: ShellParams['shell'] = {
		command: '',
		env: {},
		fail_on_nonzero: true
	};

	let envDraft = '{}';
	let envError: string | null = null;
	let lastEnvHydrationSignature = '';

	$: shell = params?.shell ?? defaultShell;
	$: cwd = String(shell.cwd ?? '');
	$: failOnNonZero = Boolean(shell.fail_on_nonzero ?? true);
	$: envHydrationSignature = JSON.stringify(shell.env ?? {});
	$: if (envHydrationSignature !== lastEnvHydrationSignature) {
		lastEnvHydrationSignature = envHydrationSignature;
		envDraft = stringifyJson(shell.env ?? {}, '{}');
		envError = null;
	}

	function validateEnvJson(text: string): { value?: Record<string, string>; error?: string } {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return { error: 'invalid JSON' };
		if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
			return { error: 'env must be a JSON object' };
		}
		const out: Record<string, string> = {};
		for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
			if (typeof value !== 'string') return { error: `env.${key} must be a string` };
			out[String(key)] = value;
		}
		return { value: out };
	}
</script>

<Section title="Shell">
	<Field label="command">
		<Input
			multiline={true}
			rows={6}
			value={shell.command ?? ''}
			onInput={(event) => onDraft({ shell: { ...shell, command: (event.currentTarget as HTMLTextAreaElement).value } })}
			onBlur={(event) => onCommit({ shell: { ...shell, command: (event.currentTarget as HTMLTextAreaElement).value } })}
		/>
	</Field>

	<Field label="cwd">
		<Input
			value={cwd}
			placeholder="C:\\work\\repo"
			onInput={(event) => {
				const value = (event.currentTarget as HTMLInputElement).value;
				onDraft({ shell: { ...shell, cwd: value.trim() === '' ? undefined : value } });
			}}
			onBlur={(event) => {
				const value = (event.currentTarget as HTMLInputElement).value;
				onCommit({ shell: { ...shell, cwd: value.trim() === '' ? undefined : value } });
			}}
		/>
	</Field>

	<Field label="fail_on_nonzero">
		<Input
			type="checkbox"
			checked={failOnNonZero}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ shell: { ...shell, fail_on_nonzero: checked } });
				onCommit({ shell: { ...shell, fail_on_nonzero: checked } });
			}}
		/>
	</Field>

	<Field label="env">
		<Input
			multiline={true}
			rows={6}
			value={envDraft}
			onInput={(event) => {
				envDraft = (event.currentTarget as HTMLTextAreaElement).value;
				envError = validateEnvJson(envDraft).error ?? null;
			}}
			onBlur={(event) => {
				envDraft = (event.currentTarget as HTMLTextAreaElement).value;
				const validated = validateEnvJson(envDraft);
				envError = validated.error ?? null;
				if (!envError && validated.value) onCommit({ shell: { ...shell, env: validated.value } });
			}}
		/>
		{#if envError}
			<div class="fieldError">{envError}</div>
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
