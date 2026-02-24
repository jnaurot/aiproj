<script lang="ts">
import type { Node } from "@xyflow/svelte";
import type { PipelineNodeData } from "$lib/flow/types";

export let selectedNode: Node<PipelineNodeData>;
export let params: Record<string, any>;
export let onDraft: (patch: Record<string, any>) => void;
export let onCommit: (patch: Record<string, any>) => void;

const jsonSchemaPlaceholder = `{"type":"object","properties":{}}`;

const asString = (v: unknown, fallback = ""): string =>
typeof v === "string" ? v : fallback;

const asNumber = (v: unknown, fallback: number): number =>
typeof v === "number" && !Number.isNaN(v) ? v : fallback;

$: baseUrl = asString(params?.baseUrl, "https://api.openai.com");
$: model = asString(params?.model, "gpt-4o-mini");
$: apiKeyRef = asString(params?.apiKeyRef, "");
$: system_prompt = asString(params?.system_prompt, "");
$: user_prompt = asString(params?.user_prompt, "Summarize the input data.");
$: temperature = asNumber(params?.temperature, 0.7);

$: outputMode = asString(params?.output?.mode, "text") as "text" | "markdown" | "json";
$: jsonSchema = params?.output?.jsonSchema;

function setStr(key: string, value: string) {
onDraft({ [key]: value });
}

function commitStr(key: string, value: string) {
onCommit({ [key]: value });
}

function setNum(key: string, value: number | undefined) {
onDraft({ [key]: value });
}

function commitNum(key: string, value: number | undefined) {
onCommit({ [key]: value });
}

function setOutputMode(next: "text" | "markdown" | "json") {
const nextOutput =
	next === "json"
	? { mode: next, jsonSchema: jsonSchema ?? {} }
	: { mode: next };
onCommit({ output: nextOutput });
}

function setJsonSchema(value: string) {
try {
	const parsed = JSON.parse(value);
	onDraft({ output: { ...(params?.output ?? { mode: "json" }), mode: "json", jsonSchema: parsed } });
} catch {
	// keep draft untouched for invalid JSON
}
}
</script>

<div class="editor">
<div class="section">
<div class="sectionTitle">OpenAI Compat</div>

<div class="grid">
	<div class="row">
	<div class="k">baseUrl</div>
	<div class="v">
		<input
		type="text"
		value={baseUrl}
		placeholder="https://api.openai.com"
		on:input={(e) => setStr("baseUrl", (e.currentTarget as HTMLInputElement).value)}
		on:blur={(e) => commitStr("baseUrl", (e.currentTarget as HTMLInputElement).value)}
		/>
	</div>
	</div>

	<div class="row">
	<div class="k">model</div>
	<div class="v">
		<input
		type="text"
		value={model}
		placeholder="gpt-4o-mini"
		on:input={(e) => setStr("model", (e.currentTarget as HTMLInputElement).value)}
		on:blur={(e) => commitStr("model", (e.currentTarget as HTMLInputElement).value)}
		/>
	</div>
	</div>

	<div class="row">
	<div class="k">apiKeyRef</div>
	<div class="v">
		<input
		type="text"
		value={apiKeyRef}
		placeholder="OPENAI_API_KEY (or direct key)"
		on:input={(e) => setStr("apiKeyRef", (e.currentTarget as HTMLInputElement).value)}
		on:blur={(e) => commitStr("apiKeyRef", (e.currentTarget as HTMLInputElement).value)}
		/>
	</div>
	</div>

	<div class="row">
	<div class="k">temperature</div>
	<div class="v">
		<input
		type="number"
		step="0.1"
		min="0"
		max="2"
		value={temperature}
		on:input={(e) => setNum("temperature", Number((e.currentTarget as HTMLInputElement).value))}
		on:blur={(e) => commitNum("temperature", Number((e.currentTarget as HTMLInputElement).value))}
		/>
	</div>
	</div>

	<div class="row">
	<div class="k">output</div>
	<div class="v">
		<select value={outputMode} on:change={(e) => setOutputMode((e.currentTarget as HTMLSelectElement).value as any)}>
		<option value="text">text</option>
		<option value="markdown">markdown</option>
		<option value="json">json</option>
		</select>
	</div>
	</div>
</div>
</div>

<div class="section">
<div class="sectionTitle">Prompt</div>

<div class="grid">
	<div class="row promptRow">
	<div class="k">system_prompt</div>
	<div class="v">
		<textarea
		rows="3"
		value={system_prompt}
		placeholder="(optional)"
		on:input={(e) => setStr("system_prompt", (e.currentTarget as HTMLTextAreaElement).value)}
		on:blur={(e) => commitStr("system_prompt", (e.currentTarget as HTMLTextAreaElement).value)}
		></textarea>
	</div>
	</div>

	<div class="row promptRow">
	<div class="k">user_prompt</div>
	<div class="v">
		<textarea
		rows="6"
		value={user_prompt}
		placeholder="Summarize the input data."
		on:input={(e) => setStr("user_prompt", (e.currentTarget as HTMLTextAreaElement).value)}
		on:blur={(e) => commitStr("user_prompt", (e.currentTarget as HTMLTextAreaElement).value)}
		></textarea>
		<div class="hint">
		Tip: you can reserve <code>{"{input}"}</code> as a placeholder for upstream text.
		</div>
	</div>
	</div>
</div>
</div>

{#if outputMode === "json"}
<div class="section">
	<div class="sectionTitle">JSON Schema</div>
	<div class="grid">
	<div class="row">
		<div class="k">jsonSchema</div>
		<div class="v">
		<textarea
			rows="8"
			placeholder={jsonSchemaPlaceholder}
			value={typeof jsonSchema === "string" ? jsonSchema : JSON.stringify(jsonSchema ?? {}, null, 2)}
			on:input={(e) => setJsonSchema((e.currentTarget as HTMLTextAreaElement).value)}
		></textarea>
		<div class="hint">
			JSON mode is enabled. Paste a JSON schema (stored as <code>output.jsonSchema</code>).
		</div>
		</div>
	</div>
	</div>
</div>
{/if}
</div>

<style>
.editor {
display: flex;
flex-direction: column;
gap: 12px;
}

.section {
border: 1px solid rgba(255, 255, 255, 0.08);
border-radius: 12px;
padding: 12px;
background: rgba(255, 255, 255, 0.03);
}

.sectionTitle {
font-weight: 650;
font-size: 14px;
margin-bottom: 10px;
opacity: 0.9;
}

.grid {
display: flex;
flex-direction: column;
gap: 10px;
}

.row {
display: grid;
grid-template-columns: 100px minmax(0, 1fr);
align-items: start;
gap: 8px;
}

.promptRow {
grid-template-columns: 1fr;
gap: 8px;
}

.k {
font-size: 14px;
opacity: 0.85;
padding-top: 8px;
}

.promptRow .k {
padding-top: 0;
}

.v {
min-width: 0;
}

input,
select,
textarea {
width: 100%;
box-sizing: border-box;
border-radius: 10px;
border: 1px solid rgba(255, 255, 255, 0.10);
background: rgba(0, 0, 0, 0.20);
color: inherit;
padding: 8px 10px;
font-size: 14px;
outline: none;
min-height: 40px;
}

textarea {
resize: vertical;
line-height: 1.35;
min-height: 96px;
}

input:focus,
select:focus,
textarea:focus {
border-color: rgba(255, 255, 255, 0.25);
}

.hint {
margin-top: 6px;
font-size: 12px;
opacity: 0.7;
}

code {
font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
font-size: 12px;
}
</style>
