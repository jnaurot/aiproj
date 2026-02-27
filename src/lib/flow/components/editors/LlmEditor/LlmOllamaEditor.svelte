<script lang="ts">
	import { onDestroy } from 'svelte';
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { LlmOutputMode, LlmParams } from '$lib/flow/schema/llm';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import {
		asNumber,
		asNumberOrEmpty,
		asString,
		parseOptionalFloat,
		parseOptionalInt,
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

	let modelOptions: string[] = [];
	let modelLoading = false;
	let modelError = '';
	let modelFetchTimer: ReturnType<typeof setTimeout> | null = null;
	let modelAbort: AbortController | null = null;
	let lastModelBaseUrl = '';

	$: void selectedNode?.id;
	$: baseUrl = asString(params?.baseUrl, 'http://localhost:11434');
	$: connectionRef = asString(params?.connectionRef, '');
	$: model = asString(params?.model, 'llama3.1:8b');
	$: system_prompt = asString(params?.system_prompt, '');
	$: user_prompt = asString(params?.user_prompt, 'Summarize the input data.');
	$: temperature = asNumber(params?.temperature, 0.7);
	$: top_p = asNumberOrEmpty(params?.top_p);
	$: max_tokens = asNumberOrEmpty(params?.max_tokens);
	$: seed = asNumberOrEmpty(params?.seed);
	$: outputMode = (asString(params?.output?.mode, 'text') as LlmOutputMode) ?? 'text';
	$: jsonSchemaText = stringifyJson(params?.output?.jsonSchema, jsonSchemaPlaceholder);

	function draft(patch: LlmPatch): void {
		onDraft?.(patch);
	}

	function commit(patch: LlmPatch): void {
		onCommit?.(patch);
	}

	function normalizeBaseUrl(url: string): string {
		return url.trim().replace(/\/+$/, '');
	}

	function mapModelsFromTags(payload: unknown): string[] {
		const raw = Array.isArray((payload as { models?: unknown[] } | undefined)?.models)
			? (payload as { models: unknown[] }).models
			: [];

		const models = raw
			.map((entry) => {
				if (typeof (entry as { name?: unknown })?.name === 'string') return (entry as { name: string }).name;
				if (typeof (entry as { model?: unknown })?.model === 'string') return (entry as { model: string }).model;
				return '';
			})
			.filter((value) => value.length > 0);

		return Array.from(new Set(models));
	}

	async function fetchOllamaModels(baseUrlInput: string): Promise<void> {
		const base = normalizeBaseUrl(baseUrlInput);
		if (!base) {
			modelOptions = [];
			modelError = '';
			modelLoading = false;
			return;
		}

		try {
			modelAbort?.abort();
			modelAbort = new AbortController();
			modelLoading = true;
			modelError = '';

			const response = await fetch(`${base}/api/tags`, {
				method: 'GET',
				signal: modelAbort.signal
			});

			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			modelOptions = mapModelsFromTags(await response.json());
		} catch (error) {
			if ((error as { name?: string } | undefined)?.name === 'AbortError') return;
			modelOptions = [];
			modelError = `Could not load models from ${base}/api/tags`;
		} finally {
			modelLoading = false;
		}
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

	$: {
		const normalized = normalizeBaseUrl(baseUrl);
		if (normalized !== lastModelBaseUrl) {
			lastModelBaseUrl = normalized;
			if (modelFetchTimer) clearTimeout(modelFetchTimer);
			modelFetchTimer = setTimeout(() => {
				void fetchOllamaModels(normalized);
			}, 250);
		}
	}

	onDestroy(() => {
		if (modelFetchTimer) clearTimeout(modelFetchTimer);
		modelAbort?.abort();
	});
</script>

<Section title="Ollama">
	<Field label="baseUrl">
		<Input
			value={baseUrl}
			placeholder="http://localhost:11434"
			onInput={(event) => draft({ baseUrl: (event.currentTarget as HTMLInputElement).value })}
			onBlur={(event) => commit({ baseUrl: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	<Field label="connectionRef">
		<Input
			value={connectionRef}
			placeholder="(optional) conn:ollama"
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
		<div class="stack">
			<select
				value={model}
				on:change={(event) => {
					const value = (event.currentTarget as HTMLSelectElement).value;
					draft({ model: value });
					commit({ model: value });
				}}
			>
				{#if model && !modelOptions.includes(model)}
					<option value={model}>{model} (current)</option>
				{/if}
				{#if modelOptions.length > 0}
					{#each modelOptions as option}
						<option value={option}>{option}</option>
					{/each}
				{:else}
					<option value={model || ''} disabled>{modelLoading ? 'Loading models...' : 'No models found'}</option>
				{/if}
			</select>
			{#if modelError}
				<div class="hint">{modelError}</div>
			{/if}
		</div>
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

	<Field label="top_p">
		<Input
			type="number"
			min="0"
			max="1"
			step="0.01"
			value={top_p}
			onInput={(event) =>
				draft({ top_p: parseOptionalFloat((event.currentTarget as HTMLInputElement).value, 0, 1) })}
			onBlur={(event) =>
				commit({ top_p: parseOptionalFloat((event.currentTarget as HTMLInputElement).value, 0, 1) })}
		/>
	</Field>

	<Field label="max_tokens">
		<Input
			type="number"
			min="1"
			step="1"
			value={max_tokens}
			onInput={(event) =>
				draft({ max_tokens: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
			onBlur={(event) =>
				commit({ max_tokens: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
		/>
	</Field>

	<Field label="seed">
		<Input
			type="number"
			step="1"
			value={seed}
			onInput={(event) => draft({ seed: parseOptionalInt((event.currentTarget as HTMLInputElement).value) })}
			onBlur={(event) => commit({ seed: parseOptionalInt((event.currentTarget as HTMLInputElement).value) })}
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
