<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type JsParams = Extract<ToolParams, { provider: 'js' }>;

	export let params: Partial<JsParams>;
	export let onDraft: (patch: Partial<JsParams>) => void;
	export let onCommit: (patch: Partial<JsParams>) => void;

	$: js = params?.js ?? { code: '' };
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
</Section>
