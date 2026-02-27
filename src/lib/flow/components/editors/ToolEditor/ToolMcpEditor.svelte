<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { stringifyJson, tryParseJson } from '$lib/flow/components/editors/shared';

	type McpParams = Extract<ToolParams, { provider: 'mcp' }>;

	export let params: Partial<McpParams>;
	export let onDraft: (patch: Partial<McpParams>) => void;
	export let onCommit: (patch: Partial<McpParams>) => void;

	$: mcp = params?.mcp ?? { serverId: 'local', toolName: '', args: {} };
	$: argsText = stringifyJson(mcp.args ?? {}, '{}');

	function commitArgs(text: string): void {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return;
		onCommit({ mcp: { ...mcp, args: parsed as Record<string, unknown> } });
	}
</script>

<Section title="MCP">
	<Field label="serverId">
		<Input
			value={mcp.serverId ?? ''}
			onInput={(event) => onDraft({ mcp: { ...mcp, serverId: (event.currentTarget as HTMLInputElement).value } })}
			onBlur={(event) => onCommit({ mcp: { ...mcp, serverId: (event.currentTarget as HTMLInputElement).value } })}
		/>
	</Field>

	<Field label="toolName">
		<Input
			value={mcp.toolName ?? ''}
			onInput={(event) => onDraft({ mcp: { ...mcp, toolName: (event.currentTarget as HTMLInputElement).value } })}
			onBlur={(event) => onCommit({ mcp: { ...mcp, toolName: (event.currentTarget as HTMLInputElement).value } })}
		/>
	</Field>

	<Field label="args">
		<Input multiline={true} rows={6} value={argsText} onBlur={(event) => commitArgs((event.currentTarget as HTMLTextAreaElement).value)} />
	</Field>
</Section>
