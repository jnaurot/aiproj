<script lang="ts">
	import KeyValueEditor from '$lib/flow/components/KeyValueEditor.svelte';
	import { graphStore } from '$lib/flow/store/graphStore';
	import type { ComponentNodeData } from '$lib/flow/types';
	import type { ComponentApiPort, ComponentCatalogItem, ComponentRevisionSummary } from '$lib/flow/client/components';

export let selectedNode: any;
export let params: Record<string, any> = {};
export let onDraft: (patch: Record<string, any>) => void = () => {};

	let components: ComponentCatalogItem[] = [];
	let revisions: ComponentRevisionSummary[] = [];
	let loadingComponents = false;
	let loadingRevisions = false;
	let loadingRevisionApply = false;
	let errorMessage = '';
	let configDraft = '{}';
	let configParseError = '';
	let autoResolvedOnce = false;
	let lastSelectedNodeId = '';

	$: componentRef = (params?.componentRef ?? {}) as {
		componentId?: string;
		revisionId?: string;
		apiVersion?: string;
	};
	$: componentId = String(componentRef.componentId ?? '').trim();
	$: revisionId = String(componentRef.revisionId ?? '').trim();
	$: apiVersion = String(componentRef.apiVersion ?? 'v1').trim() || 'v1';
	$: api = (params?.api ?? { inputs: [], outputs: [] }) as {
		inputs?: ComponentApiPort[];
		outputs?: ComponentApiPort[];
	};
	$: inputs = Array.isArray(api.inputs) ? api.inputs : [];
	$: outputs = Array.isArray(api.outputs) ? api.outputs : [];
	$: latestRevisionId = String(revisions[0]?.revisionId ?? '').trim();
	$: hasUpdate = Boolean(latestRevisionId && revisionId && latestRevisionId !== revisionId);
	$: configObj = (params?.config ?? {}) as Record<string, unknown>;
	$: if (!configParseError) {
		configDraft = JSON.stringify(configObj, null, 2);
	}

	$: bindings = (params?.bindings ?? { inputs: {}, config: {} }) as {
		inputs?: Record<string, string>;
		config?: Record<string, string>;
	};

	$: if (selectedNode?.id) {
		void ensureCatalogLoaded();
	}

	$: if (componentId) {
		void ensureRevisionsLoaded(componentId);
	} else {
		revisions = [];
	}

	$: if (String(selectedNode?.id ?? '') !== lastSelectedNodeId) {
		lastSelectedNodeId = String(selectedNode?.id ?? '');
		autoResolvedOnce = false;
	}

	async function ensureCatalogLoaded(): Promise<void> {
		if (loadingComponents || components.length > 0) return;
		loadingComponents = true;
		errorMessage = '';
		const res = await graphStore.listComponentCatalog(200, 0);
		loadingComponents = false;
		if (!(res as any)?.ok) {
			errorMessage = String((res as any)?.error ?? 'Failed to load components');
			return;
		}
		components = Array.isArray((res as any).components) ? (res as any).components : [];
		// Heal default placeholder refs (e.g. component_example@rev_1) by selecting the first available component.
		if (!componentId || !components.some((c) => c.componentId === componentId)) {
			const first = components[0];
			if (first?.componentId) {
				draftComponentRef({ componentId: first.componentId, revisionId: '' });
			}
		}
	}

	async function ensureRevisionsLoaded(cid: string): Promise<void> {
		if (!cid || loadingRevisions) return;
		loadingRevisions = true;
		errorMessage = '';
		const res = await graphStore.listComponentRevisionHistory(cid, 50, 0);
		loadingRevisions = false;
		if (!(res as any)?.ok) {
			errorMessage = String((res as any)?.error ?? 'Failed to load revisions');
			revisions = [];
			return;
		}
		revisions = Array.isArray((res as any).revisions) ? (res as any).revisions : [];
		// Auto-pick newest revision once when current revision is invalid/missing.
		if (
			!autoResolvedOnce &&
			componentId &&
			revisions.length > 0 &&
			!revisions.some((r) => r.revisionId === revisionId)
		) {
			autoResolvedOnce = true;
			await applyRevision(String(revisions[0].revisionId));
		}
	}

	function draftComponentRef(next: { componentId?: string; revisionId?: string; apiVersion?: string }): void {
		onDraft({
			componentRef: {
				componentId,
				revisionId,
				apiVersion,
				...next
			}
		});
	}

	function onBindingsInputsChange(nextInputs: Record<string, any>): void {
		onDraft({
			bindings: {
				inputs: nextInputs,
				config: { ...(bindings?.config ?? {}) }
			}
		});
	}

	function onBindingsConfigChange(nextConfigBindings: Record<string, any>): void {
		onDraft({
			bindings: {
				inputs: { ...(bindings?.inputs ?? {}) },
				config: nextConfigBindings
			}
		});
	}

	function onConfigInput(raw: string): void {
		configDraft = raw;
		try {
			const parsed = raw.trim() ? JSON.parse(raw) : {};
			configParseError = '';
			onDraft({ config: parsed });
		} catch (error) {
			configParseError = String(error);
		}
	}

	async function applyRevision(nextRevisionId: string): Promise<void> {
		if (!selectedNode?.id) return;
		if (!componentId || !nextRevisionId) return;
		loadingRevisionApply = true;
		errorMessage = '';
		const res = await graphStore.applyComponentRevisionToNode(selectedNode.id, componentId, nextRevisionId);
		loadingRevisionApply = false;
		if (!(res as any)?.ok) {
			errorMessage = String((res as any)?.error ?? 'Failed to apply revision');
			return;
		}
		await ensureRevisionsLoaded(componentId);
	}

	function typedSchemaSummary(port: ComponentApiPort): string {
		const schema = (port?.typedSchema ?? {}) as { type?: string; fields?: { name?: string; type?: string }[] };
		const base = String(schema?.type ?? 'unknown');
		const fields = Array.isArray(schema?.fields) ? schema.fields : [];
		if (fields.length === 0) return base;
		return `${base} (${fields.map((f) => `${String(f.name ?? '-')}:${String(f.type ?? 'unknown')}`).join(', ')})`;
	}
</script>

<div class="section">
	<div class="sectionTitle">Component</div>

	<div class="row">
		<div class="k">componentId</div>
		<div class="v">
			<select
				value={componentId}
				on:change={(e) => {
					const next = String((e.currentTarget as HTMLSelectElement).value ?? '').trim();
					draftComponentRef({ componentId: next, revisionId: '' });
				}}
			>
				<option value="">Select component</option>
				{#each components as item (item.componentId)}
					<option value={item.componentId}>{item.componentId}</option>
				{/each}
			</select>
		</div>
	</div>

	<div class="row">
		<div class="k">revisionId</div>
		<div class="v">
			<select
				disabled={!componentId || loadingRevisionApply}
				value={revisionId}
				on:change={(e) => applyRevision(String((e.currentTarget as HTMLSelectElement).value ?? '').trim())}
			>
				<option value="">Select revision</option>
				{#each revisions as rev (rev.revisionId)}
					<option value={rev.revisionId}>{rev.revisionId}</option>
				{/each}
			</select>
		</div>
	</div>

	<div class="row">
		<div class="k">apiVersion</div>
		<div class="v">
			<input
				value={apiVersion}
				on:input={(e) => draftComponentRef({ apiVersion: (e.currentTarget as HTMLInputElement).value })}
			/>
		</div>
	</div>

	<div class="metaRow">
		<span class="pill">pinned {revisionId || '-'}</span>
		{#if hasUpdate}
			<span class="pill warn">newer {latestRevisionId}</span>
			<button class="tabBtn" disabled={loadingRevisionApply} on:click={() => applyRevision(latestRevisionId)}>
				Upgrade revision
			</button>
		{/if}
	</div>

	{#if loadingComponents || loadingRevisions || loadingRevisionApply}
		<div class="muted">Loading...</div>
	{/if}
	{#if errorMessage}
		<div class="err">{errorMessage}</div>
	{/if}
</div>

<div class="section">
	<div class="sectionTitle">API Contract</div>
	<div class="contract">
		<div class="contractGroup">
			<div class="groupTitle">Inputs</div>
			{#if inputs.length === 0}
				<div class="muted">No input ports</div>
			{:else}
				{#each inputs as port (port.name)}
					<div class="portRow">
						<span class="mono">{port.name}</span>
						<span class="pill">{port.portType}</span>
						<span class="muted">{typedSchemaSummary(port)}</span>
					</div>
				{/each}
			{/if}
		</div>
		<div class="contractGroup">
			<div class="groupTitle">Outputs</div>
			{#if outputs.length === 0}
				<div class="muted">No output ports</div>
			{:else}
				{#each outputs as port (port.name)}
					<div class="portRow">
						<span class="mono">{port.name}</span>
						<span class="pill">{port.portType}</span>
						<span class="muted">{typedSchemaSummary(port)}</span>
					</div>
				{/each}
			{/if}
		</div>
	</div>
	<div class="muted">Ports are derived from selected revision API and are read-only here.</div>
</div>

<div class="section">
	<div class="sectionTitle">Bindings</div>
	<KeyValueEditor
		label="inputs"
		value={(bindings?.inputs ?? {}) as Record<string, any>}
		onChange={onBindingsInputsChange}
		stacked={true}
	/>
	<KeyValueEditor
		label="config"
		value={(bindings?.config ?? {}) as Record<string, any>}
		onChange={onBindingsConfigChange}
		stacked={true}
	/>
</div>

<div class="section">
	<div class="sectionTitle">Config</div>
	<textarea
		rows="4"
		value={configDraft}
		on:input={(e) => onConfigInput((e.currentTarget as HTMLTextAreaElement).value)}
	></textarea>
	{#if configParseError}
		<div class="err">Invalid JSON: {configParseError}</div>
	{/if}
</div>

<style>
	.row {
		display: grid;
		grid-template-columns: 160px minmax(0, 1fr);
		gap: 8px;
		align-items: center;
		margin-bottom: 8px;
	}

	.k {
		font-size: 12px;
		opacity: 0.8;
	}

	.v :global(input),
	.v :global(select),
	textarea {
		width: 100%;
		box-sizing: border-box;
	}

	.metaRow {
		display: flex;
		align-items: center;
		gap: 8px;
		flex-wrap: wrap;
	}

	.pill {
		font-size: 11px;
		padding: 3px 8px;
		border-radius: 999px;
		border: 1px solid #283044;
	}

	.pill.warn {
		border-color: #f2cc60;
		background: rgba(242, 204, 96, 0.12);
	}

	.contract {
		display: grid;
		gap: 10px;
	}

	.contractGroup {
		border: 1px solid #1f2430;
		border-radius: 8px;
		padding: 8px;
	}

	.groupTitle {
		font-size: 12px;
		font-weight: 600;
		margin-bottom: 6px;
	}

	.portRow {
		display: flex;
		align-items: center;
		gap: 8px;
		flex-wrap: wrap;
		margin-bottom: 4px;
	}

	.muted {
		font-size: 12px;
		opacity: 0.75;
	}

	.mono {
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 12px;
	}

	.err {
		margin-top: 6px;
		color: #ff7b72;
		font-size: 12px;
	}
</style>
