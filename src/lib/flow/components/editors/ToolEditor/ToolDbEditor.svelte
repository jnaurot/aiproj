<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { stringifyJson, tryParseJson } from '$lib/flow/components/editors/shared';

	type DbParams = Extract<ToolParams, { provider: 'db' }>;

	export let params: Partial<DbParams>;
	export let onDraft: (patch: Partial<DbParams>) => void;
	export let onCommit: (patch: Partial<DbParams>) => void;

	$: db = params?.db ?? { connectionRef: '', sql: '', params: {} };
	$: paramsText = stringifyJson(db.params ?? {}, '{}');

	function commitParams(text: string): void {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return;
		onCommit({ db: { ...db, params: parsed as Record<string, unknown> } });
	}
</script>

<Section title="DB">
	<Field label="connectionRef">
		<Input
			value={db.connectionRef ?? ''}
			onInput={(event) => onDraft({ db: { ...db, connectionRef: (event.currentTarget as HTMLInputElement).value } })}
			onBlur={(event) => onCommit({ db: { ...db, connectionRef: (event.currentTarget as HTMLInputElement).value } })}
		/>
	</Field>

	<Field label="sql">
		<Input
			multiline={true}
			rows={8}
			value={db.sql ?? ''}
			onInput={(event) => onDraft({ db: { ...db, sql: (event.currentTarget as HTMLTextAreaElement).value } })}
			onBlur={(event) => onCommit({ db: { ...db, sql: (event.currentTarget as HTMLTextAreaElement).value } })}
		/>
	</Field>

	<Field label="params">
		<Input multiline={true} rows={6} value={paramsText} onBlur={(event) => commitParams((event.currentTarget as HTMLTextAreaElement).value)} />
	</Field>
</Section>
