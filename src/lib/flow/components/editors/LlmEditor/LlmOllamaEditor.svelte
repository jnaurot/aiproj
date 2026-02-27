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
	const outputModes: LlmOutputMode[] = ['text', 'json', 'embeddings'];
	const thinkingModes = ['none', 'hidden', 'visible'] as const;
	const inputEncodings = ['text', 'json_canonical', 'table_canonical'] as const;
	const embeddingDtypes = ['float32', 'float16', 'float64'] as const;
	const embeddingLayouts = ['1d', '2d'] as const;

	let modelOptions: string[] = [];
	let modelLoading = false;
	let modelError = '';
	let modelFetchTimer: ReturnType<typeof setTimeout> | null = null;
	let modelAbort: AbortController | null = null;
	let lastModelBaseUrl = '';
	let jsonSchemaDraftText = jsonSchemaPlaceholder;

	$: void selectedNode?.id;
	$: baseUrl = asString(params?.baseUrl, 'http://192.168.12.251:11434');
	$: connectionRef = asString(params?.connectionRef, '');
	$: model = asString(params?.model, 'llama3.1:8b');
	$: system_prompt = asString(params?.system_prompt, '');
	$: user_prompt = asString(params?.user_prompt, 'Summarize the input data.');
	$: temperature = asNumber(params?.temperature, 0.7);
	$: top_p = asNumberOrEmpty(params?.top_p);
	$: max_tokens = asNumberOrEmpty(params?.max_tokens);
	$: seed = asNumberOrEmpty(params?.seed);
	$: presence_penalty = asNumberOrEmpty(params?.presence_penalty);
	$: frequency_penalty = asNumberOrEmpty(params?.frequency_penalty);
	$: repeat_penalty = asNumberOrEmpty(params?.repeat_penalty);
	$: thinkingEnabled = Boolean(params?.thinking?.enabled ?? false);
	$: thinkingMode = asString(params?.thinking?.mode, 'none');
	$: thinkingBudget = asNumberOrEmpty(params?.thinking?.budget_tokens);
	$: inputEncoding = asString(params?.inputEncoding, 'text');
	$: outputMode = (asString(params?.output?.mode, 'text') as LlmOutputMode) ?? 'text';
	$: outputStrict = params?.output?.strict ?? true;
	$: stopText = Array.isArray(params?.stop) ? params.stop.join('\n') : '';
	$: jsonSchemaText = stringifyJson(params?.output?.jsonSchema, jsonSchemaPlaceholder);
	$: embeddingDims = asNumberOrEmpty(params?.output?.embedding?.dims);
	$: embeddingDtype = asString(params?.output?.embedding?.dtype, 'float32');
	$: embeddingLayout = asString(params?.output?.embedding?.layout, '1d');
	$: {
		if (jsonSchemaDraftText !== jsonSchemaText && params?.output?.jsonSchema !== undefined) {
			jsonSchemaDraftText = jsonSchemaText;
		}
	}

	function draft(patch: LlmPatch): void {
		onDraft?.(patch);
	}

	function commit(patch: LlmPatch): void {
		onCommit?.(patch);
	}

	function thinkingPatch(patch: Partial<NonNullable<LlmParams['thinking']>>): LlmPatch {
		return {
			thinking: {
				...(params?.thinking ?? {}),
				...patch
			}
		};
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
				if (typeof (entry as { name?: unknown })?.name === 'string')
					return (entry as { name: string }).name;
				if (typeof (entry as { model?: unknown })?.model === 'string')
					return (entry as { model: string }).model;
				return '';
			})
			.filter((value) => value.length > 0);

		return Array.from(new Set(models));
	}

	function parseStopLines(raw: string): string[] | undefined {
		const parsed = raw
			.split(/\r?\n/g)
			.map((line) => line.trim())
			.filter((line) => line.length > 0);
		return parsed.length > 0 ? parsed : undefined;
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
		if (next === 'json') {
			const jsonSchema = params?.output?.jsonSchema ?? { type: 'object', properties: {} };
			commit({ output: { mode: 'json', strict: true, jsonSchema } });
			jsonSchemaDraftText = stringifyJson(jsonSchema, jsonSchemaPlaceholder);
			return;
		}
		if (next === 'embeddings') {
			commit({
				output: {
					mode: 'embeddings',
					strict: true,
					embedding: {
						dims: params?.output?.embedding?.dims ?? 1536,
						dtype: params?.output?.embedding?.dtype ?? 'float32',
						layout: params?.output?.embedding?.layout ?? '1d'
					}
				}
			});
			return;
		}
		commit({ output: { mode: 'text', strict: true } });
	}

	function setJsonSchemaDraft(raw: string): void {
		jsonSchemaDraftText = raw;
		const parsed = tryParseJson(raw);
		if (parsed === undefined) return;
		draft({
			output: {
				...(params?.output ?? { mode: 'json' }),
				mode: 'json',
				strict: outputStrict,
				jsonSchema: parsed
			}
		});
	}

	function commitJsonSchema(raw: string): void {
		const parsed = tryParseJson(raw);
		if (parsed === undefined) return;
		commit({
			output: {
				...(params?.output ?? { mode: 'json' }),
				mode: 'json',
				strict: outputStrict,
				jsonSchema: parsed
			}
		});
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
					<option value={model || ''} disabled
						>{modelLoading ? 'Loading models...' : 'No models found'}</option
					>
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
				draft({
					temperature: parseOptionalFloat((event.currentTarget as HTMLInputElement).value, 0, 2)
				})}
			onBlur={(event) =>
				commit({
					temperature: parseOptionalFloat((event.currentTarget as HTMLInputElement).value, 0, 2)
				})}
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
				commit({
					top_p: parseOptionalFloat((event.currentTarget as HTMLInputElement).value, 0, 1)
				})}
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
				commit({
					max_tokens: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1)
				})}
		/>
	</Field>

	<Field label="seed">
		<Input
			type="number"
			step="1"
			value={seed}
			onInput={(event) =>
				draft({ seed: parseOptionalInt((event.currentTarget as HTMLInputElement).value) })}
			onBlur={(event) =>
				commit({ seed: parseOptionalInt((event.currentTarget as HTMLInputElement).value) })}
		/>
	</Field>

	<Field label="stop">
		<Input
			multiline={true}
			rows={3}
			value={stopText}
			placeholder="One stop sequence per line"
			onInput={(event) =>
				draft({ stop: parseStopLines((event.currentTarget as HTMLTextAreaElement).value) })}
			onBlur={(event) =>
				commit({ stop: parseStopLines((event.currentTarget as HTMLTextAreaElement).value) })}
		/>
	</Field>

	<Field label="presence_penalty">
		<Input
			type="number"
			min="-2"
			max="2"
			step="0.1"
			value={presence_penalty}
			onInput={(event) =>
				draft({
					presence_penalty: parseOptionalFloat(
						(event.currentTarget as HTMLInputElement).value,
						-2,
						2
					)
				})}
			onBlur={(event) =>
				commit({
					presence_penalty: parseOptionalFloat(
						(event.currentTarget as HTMLInputElement).value,
						-2,
						2
					)
				})}
		/>
	</Field>

	<Field label="frequency_penalty">
		<Input
			type="number"
			min="-2"
			max="2"
			step="0.1"
			value={frequency_penalty}
			onInput={(event) =>
				draft({
					frequency_penalty: parseOptionalFloat(
						(event.currentTarget as HTMLInputElement).value,
						-2,
						2
					)
				})}
			onBlur={(event) =>
				commit({
					frequency_penalty: parseOptionalFloat(
						(event.currentTarget as HTMLInputElement).value,
						-2,
						2
					)
				})}
		/>
	</Field>

	<Field label="repeat_penalty">
		<Input
			type="number"
			min="0.5"
			max="2"
			step="0.1"
			value={repeat_penalty}
			onInput={(event) =>
				draft({
					repeat_penalty: parseOptionalFloat((event.currentTarget as HTMLInputElement).value, 0.5, 2)
				})}
			onBlur={(event) =>
				commit({
					repeat_penalty: parseOptionalFloat((event.currentTarget as HTMLInputElement).value, 0.5, 2)
				})}
		/>
	</Field>

	<Field label="thinking.enabled">
		<Input
			type="checkbox"
			checked={thinkingEnabled}
			onChange={(event) => {
				const value = (event.currentTarget as HTMLInputElement).checked;
				draft(thinkingPatch({ enabled: value }));
				commit(thinkingPatch({ enabled: value }));
			}}
		/>
	</Field>

	<Field label="thinking.mode">
		<select
			value={thinkingMode}
			on:change={(event) => {
				const value = (event.currentTarget as HTMLSelectElement)
					.value as (typeof thinkingModes)[number];
				draft(thinkingPatch({ mode: value }));
				commit(thinkingPatch({ mode: value }));
			}}
		>
			{#each thinkingModes as mode}
				<option value={mode}>{mode}</option>
			{/each}
		</select>
	</Field>

	<Field label="thinking.budget_tokens">
		<Input
			type="number"
			min="1"
			step="1"
			value={thinkingBudget}
			onInput={(event) =>
				draft(
					thinkingPatch({
						budget_tokens: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1)
					})
				)}
			onBlur={(event) =>
				commit(
					thinkingPatch({
						budget_tokens: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1)
					})
				)}
		/>
	</Field>

	<Field label="inputEncoding">
		<select
			value={inputEncoding}
			on:change={(event) => {
				const value = (event.currentTarget as HTMLSelectElement)
					.value as (typeof inputEncodings)[number];
				draft({ inputEncoding: value });
				commit({ inputEncoding: value });
			}}
		>
			{#each inputEncodings as encoding}
				<option value={encoding}>{encoding}</option>
			{/each}
		</select>
	</Field>

	<Field label="output">
		<select
			value={outputMode}
			on:change={(event) =>
				setOutputMode((event.currentTarget as HTMLSelectElement).value as LlmOutputMode)}
		>
			{#each outputModes as mode}
				<option value={mode}>{mode}</option>
			{/each}
		</select>
	</Field>

	<Field label="output.strict">
		<select
			value={String(outputStrict)}
			on:change={(event) => {
				const value = (event.currentTarget as HTMLSelectElement).value === 'true';
				draft({
					output: { ...(params?.output ?? { mode: outputMode }), mode: outputMode, strict: value }
				});
				commit({
					output: { ...(params?.output ?? { mode: outputMode }), mode: outputMode, strict: value }
				});
			}}
		>
			<option value="true">true</option>
			<option value="false">false</option>
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
			onInput={(event) =>
				draft({ system_prompt: (event.currentTarget as HTMLTextAreaElement).value })}
			onBlur={(event) =>
				commit({ system_prompt: (event.currentTarget as HTMLTextAreaElement).value })}
		/>
	</Field>

	<Field label="user_prompt">
		<div class="stack">
			<Input
				multiline={true}
				rows={6}
				value={user_prompt}
				placeholder="Summarize the input data."
				onInput={(event) =>
					draft({ user_prompt: (event.currentTarget as HTMLTextAreaElement).value })}
				onBlur={(event) =>
					commit({ user_prompt: (event.currentTarget as HTMLTextAreaElement).value })}
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
					value={jsonSchemaDraftText}
					placeholder={jsonSchemaPlaceholder}
					onInput={(event) =>
						setJsonSchemaDraft((event.currentTarget as HTMLTextAreaElement).value)}
					onBlur={(event) => commitJsonSchema((event.currentTarget as HTMLTextAreaElement).value)}
				/>
				<div class="hint">
					JSON mode is enabled. Paste a JSON schema stored as <code>output.jsonSchema</code>.
				</div>
			</div>
		</Field>
	</Section>
{/if}

{#if outputMode === 'embeddings'}
	<Section title="Embedding Contract">
		<Field label="embedding.dims">
			<Input
				type="number"
				min="1"
				step="1"
				value={embeddingDims}
				onInput={(event) =>
					draft({
						output: {
							...(params?.output ?? { mode: 'embeddings', strict: true }),
							mode: 'embeddings',
							embedding: {
								dims: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) ?? 1,
								dtype: params?.output?.embedding?.dtype ?? 'float32',
								layout: params?.output?.embedding?.layout ?? '1d'
							}
						}
					})}
				onBlur={(event) =>
					commit({
						output: {
							...(params?.output ?? { mode: 'embeddings', strict: true }),
							mode: 'embeddings',
							embedding: {
								dims: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) ?? 1,
								dtype: params?.output?.embedding?.dtype ?? 'float32',
								layout: params?.output?.embedding?.layout ?? '1d'
							}
						}
					})}
			/>
		</Field>

		<Field label="embedding.dtype">
			<select
				value={embeddingDtype}
				on:change={(event) => {
					const value = (event.currentTarget as HTMLSelectElement)
						.value as (typeof embeddingDtypes)[number];
					const patch = {
						output: {
							...(params?.output ?? { mode: 'embeddings', strict: true }),
							mode: 'embeddings' as const,
							embedding: {
								dims: params?.output?.embedding?.dims ?? 1536,
								dtype: value,
								layout: params?.output?.embedding?.layout ?? '1d'
							}
						}
					};
					draft(patch);
					commit(patch);
				}}
			>
				{#each embeddingDtypes as dtype}
					<option value={dtype}>{dtype}</option>
				{/each}
			</select>
		</Field>

		<Field label="embedding.layout">
			<select
				value={embeddingLayout}
				on:change={(event) => {
					const value = (event.currentTarget as HTMLSelectElement)
						.value as (typeof embeddingLayouts)[number];
					const patch = {
						output: {
							...(params?.output ?? { mode: 'embeddings', strict: true }),
							mode: 'embeddings' as const,
							embedding: {
								dims: params?.output?.embedding?.dims ?? 1536,
								dtype: params?.output?.embedding?.dtype ?? 'float32',
								layout: value
							}
						}
					};
					draft(patch);
					commit(patch);
				}}
			>
				{#each embeddingLayouts as layout}
					<option value={layout}>{layout}</option>
				{/each}
			</select>
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
