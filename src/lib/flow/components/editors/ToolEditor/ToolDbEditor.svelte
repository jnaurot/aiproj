<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import { graphStore, selectedNode as selectedNodeStore } from '$lib/flow/store/graphStore';
	import { getArtifactUrl, getToolDbSchema, type DbSchemaTable } from '$lib/flow/client/runs';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { stringifyJson, tryParseJson } from '$lib/flow/components/editors/shared';

	type DbParams = Extract<ToolParams, { provider: 'db' }>;

	export let params: Partial<DbParams>;
	export let onDraft: (patch: Partial<DbParams>) => void;
	export let onCommit: (patch: Partial<DbParams>) => void;

	const defaultDb: DbParams['db'] = {
		connectionRef: '',
		sql: '',
		params: {},
		capture_output: true
	};

	let connectionOpen = true;
	let queryOpen = true;
	let autoRefreshSchema = false;
	let schemaLoading = false;
	let schemaError: string | null = null;
	let discoveredTables: DbSchemaTable[] = [];
	let inputTablesFromLastRun: Array<{
		table: string;
		artifactId: string;
		rows: number;
		columns: string[];
		typedColumns: Array<{ name: string; type: string; nativeType: string }>;
	}> = [];
	let inputTablesError: string | null = null;
	let lastInputTablesSignature = '';
	let lastSchemaConnectionRef = '';
	let lastAutoRefreshRunToken = '';
	let lastRefForAutoRefresh = '';

	let paramsDraft = '{}';
	let paramsError: string | null = null;
	let lastParamsHydrationSignature = '';

	$: db = params?.db ?? defaultDb;
	$: captureOutput = Boolean(db.capture_output ?? true);
	$: connectionRef = String(db.connectionRef ?? '').trim();
	$: runToken = `${$graphStore.activeRunId ?? ''}:${$graphStore.runStatus ?? ''}`;
	$: paramsHydrationSignature = JSON.stringify(db.params ?? {});
	$: if (paramsHydrationSignature !== lastParamsHydrationSignature) {
		lastParamsHydrationSignature = paramsHydrationSignature;
		paramsDraft = stringifyJson(db.params ?? {}, '{}');
		paramsError = null;
	}

	$: if (autoRefreshSchema && connectionRef && connectionRef !== lastRefForAutoRefresh) {
		lastRefForAutoRefresh = connectionRef;
		void refreshSchema();
	}

	$: if (
		autoRefreshSchema &&
		connectionRef &&
		$graphStore.runStatus === 'running' &&
		runToken !== lastAutoRefreshRunToken
	) {
		lastAutoRefreshRunToken = runToken;
		void refreshSchema();
	}

	function validateParamsJson(text: string): { value?: Record<string, unknown>; error?: string } {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return { error: 'invalid JSON' };
		if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
			return { error: 'params must be a JSON object' };
		}
		return { value: parsed as Record<string, unknown> };
	}

	function autoGrow(event: Event): void {
		const target = event.currentTarget as HTMLTextAreaElement | null;
		if (!target) return;
		target.style.height = 'auto';
		target.style.height = `${Math.max(96, target.scrollHeight)}px`;
	}

	async function refreshSchema(): Promise<void> {
		const ref = String(db.connectionRef ?? '').trim();
		if (!ref) {
			discoveredTables = [];
			schemaError = 'connectionRef is required to load schema';
			return;
		}
		schemaLoading = true;
		schemaError = null;
		try {
			const schema = await getToolDbSchema(ref);
			discoveredTables = schema.tables ?? [];
			lastSchemaConnectionRef = schema.connectionRef;
		} catch (error) {
			discoveredTables = [];
			schemaError = error instanceof Error ? error.message : String(error);
		} finally {
			schemaLoading = false;
		}
	}

	$: if (connectionRef && lastSchemaConnectionRef === '') {
		void refreshSchema();
	}

	function artifactIdFromBinding(binding: any): string {
		const candidates = [
			binding?.current?.artifactId,
			binding?.currentArtifactId,
			binding?.last?.artifactId,
			binding?.lastArtifactId
		];
		for (const c of candidates) {
			const id = String(c ?? '').trim();
			if (id) return id;
		}
		return '';
	}

	async function refreshInputTablesFromLatestOutput(): Promise<void> {
		const nodeId = String($selectedNodeStore?.id ?? '').trim();
		const graphId = String($graphStore.graphId ?? '').trim();
		if (!nodeId || !graphId) {
			inputTablesFromLastRun = [];
			inputTablesError = null;
			return;
		}
		const binding = $graphStore.nodeBindings?.[nodeId];
		const artifactId = artifactIdFromBinding(binding);
		if (!artifactId) {
			inputTablesFromLastRun = [];
			inputTablesError = null;
			return;
		}
		try {
			const res = await fetch(getArtifactUrl(artifactId, graphId));
			if (!res.ok) {
				inputTablesFromLastRun = [];
				inputTablesError = `artifact fetch failed: ${res.status}`;
				return;
			}
			const body = await res.json();
			const payload = (body?.payload ?? {}) as Record<string, unknown>;
			const result = (payload?.result ?? {}) as Record<string, unknown>;
			const raw = Array.isArray(result?.inputTables) ? result.inputTables : [];
			inputTablesFromLastRun = raw
				.map((entry) => {
					const row = entry as Record<string, unknown>;
					return {
						table: String(row.table ?? '').trim(),
						artifactId: String(row.artifactId ?? '').trim(),
						rows: Number(row.rows ?? 0),
						columns: Array.isArray(row.columns) ? row.columns.map((c) => String(c)) : [],
						typedColumns: Array.isArray(row.typedColumns)
							? row.typedColumns
									.map((tc) => {
										const t = tc as Record<string, unknown>;
										return {
											name: String(t.name ?? '').trim(),
											type: String(t.type ?? 'unknown').trim() || 'unknown',
											nativeType: String(t.nativeType ?? 'unknown').trim() || 'unknown'
										};
									})
									.filter((tc) => tc.name.length > 0)
							: []
					};
				})
				.filter((entry) => entry.table.length > 0);
			inputTablesError = null;
		} catch (error) {
			inputTablesFromLastRun = [];
			inputTablesError = error instanceof Error ? error.message : String(error);
		}
	}

	$: inputTablesSignature = `${String($selectedNodeStore?.id ?? '')}|${String($graphStore.graphId ?? '')}|${artifactIdFromBinding($graphStore.nodeBindings?.[$selectedNodeStore?.id ?? ''])}`;
	$: if (inputTablesSignature && inputTablesSignature !== lastInputTablesSignature) {
		lastInputTablesSignature = inputTablesSignature;
		void refreshInputTablesFromLatestOutput();
	}
</script>

<Section title="DB">
	<details class="group" bind:open={connectionOpen}>
		<summary>Connection</summary>
		<div class="groupBody">
			<Field label="connectionRef">
				<Input
					value={db.connectionRef ?? ''}
					placeholder="duckdb:///C:/data/my.duckdb or :memory:"
					onInput={(event) => onDraft({ db: { ...db, connectionRef: (event.currentTarget as HTMLInputElement).value } })}
					onBlur={(event) => onCommit({ db: { ...db, connectionRef: (event.currentTarget as HTMLInputElement).value } })}
				/>
			</Field>

			<div class="schemaToolbar">
				<button type="button" on:click={refreshSchema} disabled={schemaLoading || !connectionRef}>
					{schemaLoading ? 'Refreshing...' : 'Refresh schema'}
				</button>
				<label class="toggle">
					<input
						type="checkbox"
						checked={autoRefreshSchema}
						on:change={(event) => {
							autoRefreshSchema = (event.currentTarget as HTMLInputElement).checked;
						}}
					/>
					<span>auto-refresh on run start</span>
				</label>
			</div>

			{#if schemaError}
				<div class="fieldError">{schemaError}</div>
			{:else if discoveredTables.length === 0}
				<div class="schemaHint">No user tables/views discovered yet.</div>
			{:else}
				<div class="schemaList">
					{#each discoveredTables as table}
						<details class="tableBlock">
							<summary>{table.schema}.{table.name} <span class="kindPill">{table.kind}</span></summary>
							<div class="cols">
								{#if table.columns.length === 0}
									<div class="schemaHint">No columns.</div>
								{:else}
									{#each table.columns as column}
										<div class="colRow">
											<span class="colName">{column.name}</span>
											<span class="colType">{column.normalizedType}</span>
											<span class="colNative">{column.nativeType}</span>
										</div>
									{/each}
								{/if}
							</div>
						</details>
					{/each}
				</div>
			{/if}

			{#if inputTablesError}
				<div class="fieldError">{inputTablesError}</div>
			{:else if inputTablesFromLastRun.length > 0}
				<div class="schemaHint">Upstream input tables (from latest Tool output)</div>
				<div class="schemaList">
					{#each inputTablesFromLastRun as table}
						<details class="tableBlock">
							<summary>{table.table} <span class="kindPill">rows {table.rows}</span></summary>
							<div class="cols">
								{#if table.columns.length === 0}
									<div class="schemaHint">No columns.</div>
								{:else}
									{#each table.columns as column}
										{@const typed = table.typedColumns.find((tc) => tc.name === column)}
										<div class="colRow">
											<span class="colName">{column}</span>
											<span class="colType">{typed?.type ?? 'unknown'}</span>
											<span class="colNative">{typed?.nativeType ?? 'artifact'}</span>
										</div>
									{/each}
								{/if}
							</div>
						</details>
					{/each}
				</div>
			{/if}
		</div>
	</details>

	<details class="group" bind:open={queryOpen}>
		<summary>Query</summary>
		<div class="groupBody">
			<Field label="sql">
				<Input
					multiline={true}
					rows={4}
					value={db.sql ?? ''}
					onInput={(event) => {
						autoGrow(event);
						onDraft({ db: { ...db, sql: (event.currentTarget as HTMLTextAreaElement).value } });
					}}
					onBlur={(event) => {
						autoGrow(event);
						onCommit({ db: { ...db, sql: (event.currentTarget as HTMLTextAreaElement).value } });
					}}
				/>
			</Field>

			<Field label="params">
				<Input
					multiline={true}
					rows={4}
					value={paramsDraft}
					onInput={(event) => {
						autoGrow(event);
						paramsDraft = (event.currentTarget as HTMLTextAreaElement).value;
						paramsError = validateParamsJson(paramsDraft).error ?? null;
					}}
					onBlur={(event) => {
						autoGrow(event);
						paramsDraft = (event.currentTarget as HTMLTextAreaElement).value;
						const validated = validateParamsJson(paramsDraft);
						paramsError = validated.error ?? null;
						if (!paramsError && validated.value) onCommit({ db: { ...db, params: validated.value } });
					}}
				/>
				{#if paramsError}
					<div class="fieldError">{paramsError}</div>
				{/if}
			</Field>

			<Field label="capture_output">
				<Input
					type="checkbox"
					checked={captureOutput}
					onChange={(event) => {
						const checked = (event.currentTarget as HTMLInputElement).checked;
						onDraft({ db: { ...db, capture_output: checked } });
						onCommit({ db: { ...db, capture_output: checked } });
					}}
				/>
			</Field>
		</div>
	</details>
</Section>

<style>
	.group {
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 10px;
		padding: 8px 10px;
		margin-bottom: 10px;
	}

	summary {
		cursor: pointer;
		font-weight: 600;
	}

	.groupBody {
		margin-top: 10px;
	}

	.schemaToolbar {
		display: flex;
		gap: 10px;
		align-items: center;
		margin-bottom: 8px;
	}

	.schemaToolbar button {
		border: 1px solid rgba(255, 255, 255, 0.12);
		border-radius: 8px;
		padding: 6px 10px;
		background: transparent;
		color: inherit;
	}

	.toggle {
		display: inline-flex;
		align-items: center;
		gap: 8px;
		font-size: 12px;
		opacity: 0.9;
	}

	.schemaList {
		display: grid;
		gap: 8px;
	}

	.tableBlock {
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 8px;
		padding: 6px 8px;
	}

	.kindPill {
		font-size: 11px;
		opacity: 0.75;
		margin-left: 8px;
	}

	.cols {
		display: grid;
		gap: 6px;
		margin-top: 8px;
	}

	.colRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto auto;
		gap: 8px;
		font-size: 12px;
		align-items: center;
	}

	.colName {
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New',
			monospace;
	}

	.colType {
		padding: 2px 6px;
		border: 1px solid rgba(255, 255, 255, 0.12);
		border-radius: 999px;
	}

	.colNative {
		font-size: 11px;
		opacity: 0.75;
	}

	.schemaHint {
		font-size: 12px;
		opacity: 0.75;
	}

	.fieldError {
		margin-top: 6px;
		font-size: 12px;
		color: #f87171;
	}
</style>
