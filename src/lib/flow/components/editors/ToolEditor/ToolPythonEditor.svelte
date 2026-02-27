<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type PythonParams = Extract<ToolParams, { provider: 'python' }>;

	export let params: Partial<PythonParams>;
	export let onDraft: (patch: Partial<PythonParams>) => void;
	export let onCommit: (patch: Partial<PythonParams>) => void;

	$: python = params?.python ?? { code: '' };
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
</Section>
