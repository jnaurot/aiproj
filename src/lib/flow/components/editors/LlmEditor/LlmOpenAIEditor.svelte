<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { LlmOutputMode, LlmParams } from '$lib/flow/schema/llm';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import {
		asNumber,
		asString,
		parseOptionalFloat,
		stringifyJson,
		tryParseJson
	} from '$lib/flow/components/editors/shared';

	type LlmPatch = Partial<LlmParams>;

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<LlmParams>;
	export let onDraft: (patch: LlmPatch) => void;
	export let onCommit: (patch: LlmPatch) => void;

	const jsonSchemaPlaceholder = `{"type":"object","properties":{}}`;
	const outputModes: LlmOutputMode[] = ['text', 'markdown', 'json'];

	$: void selectedNode?.id;
	$: baseUrl = asString(params?.baseUrl, 'https://api.openai.com');
	$: connectionRef = asString(params?.connectionRef, '');
	$: model = asString(params?.model, 'gpt-4o-mini');
	$: system_prompt = asString(params?.system_prompt, '');
	$: user_prompt = asString(params?.user_prompt, 'Summarize the input data.');
	$: temperature = asNumber(params?.temperature, 0.7);
	$: outputMode = (asString(params?.output?.mode, 'text') as LlmOutputMode) ?? 'text';
	$: jsonSchemaText = stringifyJson(params?.output?.jsonSchema, jsonSchemaPlaceholder);

	function draft(patch: LlmPatch): void {
		onDraft?.(patch);
	}

	function commit(patch: LlmPatch): void {
		onCommit?.(patch);
	}

	function setOutputMode(next: LlmOutputMode): void {
		const output =
			next === 'json'
				? { mode: next, jsonSchema: params?.output?.jsonSchema ?? {} }
				: { mode: next };
		commit({ output });
	}

	function setJsonSchema(raw: string): void {
		const parsed = tryParseJson(raw);
		if (parsed === undefined) return;
		draft({ output: { ...(params?.output ?? { mode: 'json' }), mode: 'json', jsonSchema: parsed } });
	}
</script>

<Section title="OpenAI Compat">
	<Field label="baseUrl">
		<Input
			value={baseUrl}
			placeholder="https://api.openai.com"
			onInput={(event) => draft({ baseUrl: (event.currentTarget as HTMLInputElement).value })}
			onBlur={(event) => commit({ baseUrl: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	<Field label="connectionRef">
		<Input
			value={connectionRef}
			placeholder="OPENAI_API_KEY or conn:openai"
			onInput={(event) => {
				const value = (event.currentTarget as HTMLInputElement).value.trim();
				draft({ connectionRef: value === '' ? undefined : value });
			}}
			onBlur={(event) => {
				const value = (event.currentTarget as HTMLInputElement).value.trim();
				commit({ connectionRef: value === '' ? undefined : value });
			}}
		/>
	</Field>

	<Field label="model">
		<Input
			value={model}
			placeholder="gpt-4o-mini"
			onInput={(event) => draft({ model: (event.currentTarget as HTMLInputElement).value })}
			onBlur={(event) => commit({ model: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	<Field label="temperature">
		<Input
			type="number"
			min="0"
			max="2"
			step="0.1"
			value={temperature}
			onInput={(event) =>
				draft({ temperature: parseOptionalFloat((event.currentTarget as HTMLInputElement).value, 0, 2) })}
			onBlur={(event) =>
				commit({ temperature: parseOptionalFloat((event.currentTarget as HTMLInputElement).value, 0, 2) })}
		/>
	</Field>

	<Field label="output">
		<select
			value={outputMode}
			on:change={(event) => setOutputMode((event.currentTarget as HTMLSelectElement).value as LlmOutputMode)}
		>
			{#each outputModes as mode}
				<option value={mode}>{mode}</option>
			{/each}
		</select>
	</Field>
</Section>

<Section title="Prompt">
	<Field label="system_prompt">
		<Input
			multiline={true}
			rows={3}
			value={system_prompt}
			placeholder="(optional)"
			onInput={(event) => draft({ system_prompt: (event.currentTarget as HTMLTextAreaElement).value })}
			onBlur={(event) => commit({ system_prompt: (event.currentTarget as HTMLTextAreaElement).value })}
		/>
	</Field>

	<Field label="user_prompt">
		<div class="stack">
			<Input
				multiline={true}
				rows={6}
				value={user_prompt}
				placeholder="Summarize the input data."
				onInput={(event) => draft({ user_prompt: (event.currentTarget as HTMLTextAreaElement).value })}
				onBlur={(event) => commit({ user_prompt: (event.currentTarget as HTMLTextAreaElement).value })}
			/>
			<div class="hint">
				Tip: you can reserve <code>{'{input}'}</code> as a placeholder for upstream text.
			</div>
		</div>
	</Field>
</Section>

{#if outputMode === 'json'}
	<Section title="JSON Schema">
		<Field label="jsonSchema">
			<div class="stack">
				<Input
					multiline={true}
					rows={8}
					value={jsonSchemaText}
					placeholder={jsonSchemaPlaceholder}
					onInput={(event) => setJsonSchema((event.currentTarget as HTMLTextAreaElement).value)}
				/>
				<div class="hint">
					JSON mode is enabled. Paste a JSON schema stored as <code>output.jsonSchema</code>.
				</div>
			</div>
		</Field>
	</Section>
{/if}

<style>
	.stack {
		display: flex;
		flex-direction: column;
		gap: 6px;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
	}

	code {
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 12px;
	}
</style>
