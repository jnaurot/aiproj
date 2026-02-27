<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { stringifyJson, tryParseJson } from '$lib/flow/components/editors/shared';

	type BuiltinParams = Extract<ToolParams, { provider: 'builtin' }>;

	export let params: Partial<BuiltinParams>;
	export let onDraft: (patch: Partial<BuiltinParams>) => void;
	export let onCommit: (patch: Partial<BuiltinParams>) => void;

	$: builtin = params?.builtin ?? { toolId: '', args: {} };
	$: argsText = stringifyJson(builtin.args ?? {}, '{}');

	function commitArgs(text: string): void {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return;
		onCommit({ builtin: { ...builtin, args: parsed as Record<string, unknown> } });
	}
</script>

<Section title="Builtin">
	<Field label="toolId">
		<Input
			value={builtin.toolId ?? ''}
			onInput={(event) => onDraft({ builtin: { ...builtin, toolId: (event.currentTarget as HTMLInputElement).value } })}
			onBlur={(event) => onCommit({ builtin: { ...builtin, toolId: (event.currentTarget as HTMLInputElement).value } })}
		/>
	</Field>

	<Field label="args">
		<Input multiline={true} rows={6} value={argsText} onBlur={(event) => commitArgs((event.currentTarget as HTMLTextAreaElement).value)} />
	</Field>
</Section>
