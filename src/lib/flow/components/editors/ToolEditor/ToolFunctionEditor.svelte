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

	$: fn = params?.function ?? { module: '', export: '', args: {} };
	$: argsText = stringifyJson(fn.args ?? {}, '{}');

	function commitArgs(text: string): void {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return;
		onCommit({ function: { ...fn, args: parsed as Record<string, unknown> } });
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

	<Field label="args">
		<Input multiline={true} rows={6} value={argsText} onBlur={(event) => commitArgs((event.currentTarget as HTMLTextAreaElement).value)} />
	</Field>
</Section>
