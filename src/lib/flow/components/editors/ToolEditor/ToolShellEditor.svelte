<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type ShellParams = Extract<ToolParams, { provider: 'shell' }>;

	export let params: Partial<ShellParams>;
	export let onDraft: (patch: Partial<ShellParams>) => void;
	export let onCommit: (patch: Partial<ShellParams>) => void;

	$: shell = params?.shell ?? { command: '' };
</script>

<Section title="Shell">
	<Field label="command">
		<Input
			value={shell.command ?? ''}
			onInput={(event) => onDraft({ shell: { ...shell, command: (event.currentTarget as HTMLInputElement).value } })}
			onBlur={(event) => onCommit({ shell: { ...shell, command: (event.currentTarget as HTMLInputElement).value } })}
		/>
	</Field>
</Section>
