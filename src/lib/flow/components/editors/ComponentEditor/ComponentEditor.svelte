<script lang="ts">
	import KeyValueEditor from '$lib/flow/components/KeyValueEditor.svelte';
	import ToolbarMenu from '$lib/flow/components/ToolbarMenu.svelte';
	import type { ToolbarMenuItem } from '$lib/flow/components/toolbarMenu';
	import { graphStore } from '$lib/flow/store/graphStore';
	import type { ComponentNodeData } from '$lib/flow/types';
	import type { ComponentApiPort, ComponentCatalogItem, ComponentRevisionSummary } from '$lib/flow/client/components';
	import type { PortType } from '$lib/flow/types';

export let selectedNode: any;
export let params: Record<string, any> = {};
export let onDraft: (patch: Record<string, any>) => void = () => {};

	let components: ComponentCatalogItem[] = [];
	let revisions: ComponentRevisionSummary[] = [];
	let loadingComponents = false;
	let loadingRevisions = false;
	let loadingRevisionApply = false;
	let loadingRevisionDetail = false;
	let mutatingComponent = false;
	let errorMessage = '';
	let configDraft = '{}';
	let configParseError = '';
	let lastSelectedNodeId = '';
	let lastRevisionDetailKey = '';
	let internalNodeOptions: Array<{ id: string; label: string }> = [];

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
	const PORT_TYPE_OPTIONS: PortType[] = ['table', 'text', 'json', 'binary', 'embeddings'];
	const TYPED_TYPE_OPTIONS = ['table', 'json', 'text', 'binary', 'embeddings', 'unknown'] as const;
	let outputFieldsJsonErrors: Record<number, string> = {};
	$: latestRevisionId = String(revisions[0]?.revisionId ?? '').trim();
	$: hasUpdate = Boolean(latestRevisionId && revisionId && latestRevisionId !== revisionId);
	$: configObj = (params?.config ?? {}) as Record<string, unknown>;
	$: if (!configParseError) {
		configDraft = JSON.stringify(configObj, null, 2);
	}

	$: bindings = (params?.bindings ?? { inputs: {}, config: {} }) as {
		inputs?: Record<string, string>;
		config?: Record<string, string>;
		outputs?: Record<string, { nodeId?: string; artifact?: 'current' | 'last' }>;
	};
	$: outputBindings = (bindings?.outputs ?? {}) as Record<string, { nodeId?: string; artifact?: 'current' | 'last' }>;

	$: if (selectedNode?.id) {
		void ensureCatalogLoaded();
	}

	$: if (componentId) {
		void ensureRevisionsLoaded(componentId);
	} else {
		revisions = [];
	}
	$: if (componentId && revisionId) {
		void ensureRevisionDetailLoaded(componentId, revisionId);
	} else {
		internalNodeOptions = [];
		lastRevisionDetailKey = '';
	}

	$: if (String(selectedNode?.id ?? '') !== lastSelectedNodeId) {
		lastSelectedNodeId = String(selectedNode?.id ?? '');
	}
	$: componentActionItems = [
		{
			id: 'rename',
			label: 'Rename',
			disabled: !componentId || mutatingComponent
		},
		{
			id: 'edit_internals',
			label: 'Edit internals',
			disabled: !componentId || !revisionId || mutatingComponent
		},
		{
			id: 'fork',
			label: 'Fork',
			disabled: !componentId || !revisionId || mutatingComponent
		},
		{
			id: 'delete',
			label: 'Delete',
			disabled: !componentId || mutatingComponent,
			danger: true
		},
		{
			id: 'delete_revision',
			label: 'Delete revision',
			disabled: !componentId || !revisionId || mutatingComponent,
			danger: true
		}
	] satisfies ToolbarMenuItem[];

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
		if (componentId && !components.some((c) => c.componentId === componentId)) {
			errorMessage = `Pinned component not found in catalog: ${componentId}`;
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
		if (revisionId && !revisions.some((r) => r.revisionId === revisionId)) {
			errorMessage = `Pinned revision not found: ${componentId}@${revisionId}`;
		}
	}

	async function ensureRevisionDetailLoaded(cid: string, rid: string): Promise<void> {
		const key = `${cid}@${rid}`;
		if (!cid || !rid) {
			internalNodeOptions = [];
			lastRevisionDetailKey = '';
			return;
		}
		if (loadingRevisionDetail) return;
		if (lastRevisionDetailKey === key) return;
		loadingRevisionDetail = true;
		const res = await graphStore.getComponentRevisionDetail(cid, rid);
		loadingRevisionDetail = false;
		if (!(res as any)?.ok) {
			internalNodeOptions = [];
			lastRevisionDetailKey = '';
			return;
		}
		const detail = (res as any).detail ?? {};
		const graph = (detail?.definition?.graph ?? {}) as { nodes?: any[] };
		const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
		internalNodeOptions = nodes
			.map((n) => {
				const id = String(n?.id ?? '').trim();
				if (!id) return null;
				const kind = String(n?.data?.kind ?? 'node').trim();
				const name = String(n?.data?.label ?? id).trim() || id;
				return {
					id,
					label: `${kind}: ${name}`
				};
			})
			.filter((x): x is { id: string; label: string } => Boolean(x))
			.sort((a, b) => a.label.localeCompare(b.label));
		lastRevisionDetailKey = key;
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
				config: { ...(bindings?.config ?? {}) },
				outputs: { ...(bindings?.outputs ?? {}) }
			}
		});
	}

	function onBindingsConfigChange(nextConfigBindings: Record<string, any>): void {
		onDraft({
			bindings: {
				inputs: { ...(bindings?.inputs ?? {}) },
				config: nextConfigBindings,
				outputs: { ...(bindings?.outputs ?? {}) }
			}
		});
	}

	function onOutputBindingNodeIdChange(name: string, nodeId: string): void {
		const nextOutputs = { ...(bindings?.outputs ?? {}) } as Record<string, { nodeId?: string; artifact?: 'current' | 'last' }>;
		const current = nextOutputs[name] ?? { artifact: 'current' as const };
		nextOutputs[name] = { ...current, nodeId: String(nodeId ?? '').trim() };
		onDraft({
			bindings: {
				inputs: { ...(bindings?.inputs ?? {}) },
				config: { ...(bindings?.config ?? {}) },
				outputs: nextOutputs
			}
		});
	}

	function onOutputBindingArtifactChange(name: string, artifact: string): void {
		const mode = artifact === 'last' ? 'last' : 'current';
		const nextOutputs = { ...(bindings?.outputs ?? {}) } as Record<string, { nodeId?: string; artifact?: 'current' | 'last' }>;
		const current = nextOutputs[name] ?? {};
		nextOutputs[name] = { ...current, artifact: mode };
		onDraft({
			bindings: {
				inputs: { ...(bindings?.inputs ?? {}) },
				config: { ...(bindings?.config ?? {}) },
				outputs: nextOutputs
			}
		});
	}

	function draftApiOutputs(nextOutputs: ComponentApiPort[]): void {
		onDraft({
			api: {
				inputs: [...inputs],
				outputs: nextOutputs
			}
		});
	}

	function updateApiOutput(
		index: number,
		patch: Partial<ComponentApiPort>
	): void {
		const next = outputs.map((out, i) => (i === index ? { ...out, ...patch } : out));
		draftApiOutputs(next as ComponentApiPort[]);
	}

	function addApiOutput(): void {
		const nextName = `out_${outputs.length + 1}`;
		const next = [
			...outputs,
			{
				name: nextName,
				portType: 'json',
				required: true,
				typedSchema: { type: 'json', fields: [] }
			}
		];
		draftApiOutputs(next as ComponentApiPort[]);
	}

	function removeApiOutput(index: number): void {
		const removed = outputs[index];
		const next = outputs.filter((_, i) => i !== index);
		draftApiOutputs(next as ComponentApiPort[]);
		if (removed?.name) {
			const nextBindings = { ...(bindings?.outputs ?? {}) } as Record<string, { nodeId?: string; artifact?: 'current' | 'last' }>;
			delete nextBindings[String(removed.name)];
			onDraft({
				bindings: {
					inputs: { ...(bindings?.inputs ?? {}) },
					config: { ...(bindings?.config ?? {}) },
					outputs: nextBindings
				}
			});
		}
		const nextErrors = { ...outputFieldsJsonErrors };
		delete nextErrors[index];
		outputFieldsJsonErrors = nextErrors;
	}

	function onApiOutputFieldsJsonChange(index: number, raw: string): void {
		try {
			const parsed = raw.trim() ? JSON.parse(raw) : [];
			if (!Array.isArray(parsed)) {
				outputFieldsJsonErrors = {
					...outputFieldsJsonErrors,
					[index]: 'typedSchema.fields must be a JSON array'
				};
				return;
			}
			const normalized = parsed.map((f) => ({
				name: String((f as any)?.name ?? '').trim(),
				type: String((f as any)?.type ?? 'unknown').trim() || 'unknown',
				nativeType:
					(f as any)?.nativeType == null ? undefined : String((f as any).nativeType),
				nullable: Boolean((f as any)?.nullable ?? false)
			}));
			updateApiOutput(index, {
				typedSchema: {
					...(outputs[index]?.typedSchema ?? { type: 'unknown', fields: [] }),
					fields: normalized
				}
			});
			const nextErrors = { ...outputFieldsJsonErrors };
			delete nextErrors[index];
			outputFieldsJsonErrors = nextErrors;
		} catch (error) {
			outputFieldsJsonErrors = {
				...outputFieldsJsonErrors,
				[index]: String(error)
			};
		}
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
		await ensureRevisionDetailLoaded(componentId, nextRevisionId);
	}

	async function refreshCatalogAndRevisions(nextComponentId?: string): Promise<void> {
		components = [];
		revisions = [];
		await ensureCatalogLoaded();
		const chosen = String(nextComponentId ?? componentId).trim();
		if (chosen) {
			await ensureRevisionsLoaded(chosen);
		}
	}

	async function renameCurrentComponent(): Promise<void> {
		if (!componentId) return;
		const next = (window.prompt('Rename component id', componentId) ?? '').trim();
		if (!next || next === componentId) return;
		mutatingComponent = true;
		errorMessage = '';
		const res = await graphStore.renameComponent(componentId, next);
		mutatingComponent = false;
		if (!(res as any)?.ok) {
			errorMessage = String((res as any)?.error ?? 'Rename failed');
			return;
		}
		await refreshCatalogAndRevisions(next);
	}

	async function forkCurrentComponent(): Promise<void> {
		if (!selectedNode?.id) return;
		if (!componentId || !revisionId) return;
		const suggested = `${componentId}_copy`.slice(0, 96);
		const nextComponentId = (window.prompt('Fork to new component id', suggested) ?? '').trim();
		if (!nextComponentId || nextComponentId === componentId) return;
		const nextRevisionId = (window.prompt('Fork revision id (optional)', '') ?? '').trim();
		const note =
			(window.prompt(
				'Fork message (optional)',
				`fork:${componentId}@${String(revisionId).slice(0, 16)}`
			) ?? '').trim() || undefined;
		mutatingComponent = true;
		errorMessage = '';
		const res = await graphStore.forkComponentRevisionToNode(
			selectedNode.id,
			componentId,
			revisionId,
			nextComponentId,
			{
				revisionId: nextRevisionId || undefined,
				message: note
			}
		);
		mutatingComponent = false;
		if (!(res as any)?.ok) {
			errorMessage = String((res as any)?.error ?? (res as any)?.reason ?? 'Fork failed');
			return;
		}
		await refreshCatalogAndRevisions(nextComponentId);
	}

	async function editCurrentInternals(): Promise<void> {
		if (!componentId || !revisionId) return;
		const confirmed = window.confirm(
			`Load internals for ${componentId}@${revisionId} into the canvas? Current graph view will be replaced.`
		);
		if (!confirmed) return;
		mutatingComponent = true;
		errorMessage = '';
		const res = await graphStore.openComponentRevisionForEditing(componentId, revisionId);
		mutatingComponent = false;
		if (!(res as any)?.ok) {
			errorMessage = String((res as any)?.error ?? (res as any)?.reason ?? 'Edit internals failed');
		}
	}

	async function deleteCurrentComponent(): Promise<void> {
		if (!componentId) return;
		const confirmed = window.confirm(
			`Delete component "${componentId}" and all its revisions? This cannot be undone.`
		);
		if (!confirmed) return;
		mutatingComponent = true;
		errorMessage = '';
		const res = await graphStore.deleteComponent(componentId);
		mutatingComponent = false;
		if (!(res as any)?.ok) {
			errorMessage = String((res as any)?.error ?? 'Delete failed');
			return;
		}
		draftComponentRef({ componentId: '', revisionId: '' });
		await refreshCatalogAndRevisions('');
		const first = components[0];
		if (first?.componentId) {
			draftComponentRef({ componentId: first.componentId, revisionId: '' });
			await ensureRevisionsLoaded(first.componentId);
		}
	}

	async function deleteCurrentRevision(): Promise<void> {
		if (!componentId || !revisionId) return;
		const confirmed = window.confirm(
			`Delete revision "${revisionId}" from component "${componentId}"?`
		);
		if (!confirmed) return;
		mutatingComponent = true;
		errorMessage = '';
		const res = await graphStore.deleteComponentRevision(componentId, revisionId);
		mutatingComponent = false;
		if (!(res as any)?.ok) {
			errorMessage = String((res as any)?.error ?? 'Delete revision failed');
			return;
		}
		const payload = (res as any).deleted ?? {};
		const componentDeleted = Boolean(payload?.componentDeleted);
		const remainingLatestRevisionId = String(payload?.remainingLatestRevisionId ?? '').trim();
		if (componentDeleted) {
			draftComponentRef({ componentId: '', revisionId: '' });
			await refreshCatalogAndRevisions('');
			const first = components[0];
			if (first?.componentId) {
				draftComponentRef({ componentId: first.componentId, revisionId: '' });
				await ensureRevisionsLoaded(first.componentId);
			}
			return;
		}
		await refreshCatalogAndRevisions(componentId);
		if (remainingLatestRevisionId) {
			await applyRevision(remainingLatestRevisionId);
		}
	}

	function onComponentAction(actionId: string): void {
		if (actionId === 'rename') {
			void renameCurrentComponent();
			return;
		}
		if (actionId === 'edit_internals') {
			void editCurrentInternals();
			return;
		}
		if (actionId === 'fork') {
			void forkCurrentComponent();
			return;
		}
		if (actionId === 'delete') {
			void deleteCurrentComponent();
			return;
		}
		if (actionId === 'delete_revision') {
			void deleteCurrentRevision();
		}
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

	<div class="row rowNoLabel">
		<div class="v">
			<div class="idRow">
				<div class="idValue" title={componentId || '-'}>{componentId || '-'}</div>
				<ToolbarMenu
					label="Actions"
					items={componentActionItems}
					disabled={!componentId || mutatingComponent}
					menuAriaLabel="Component actions"
					align="right"
					compact={true}
					onSelect={onComponentAction}
				/>
			</div>
		</div>
	</div>

	<div class="row rowNoLabel">
		<div class="v">
			<div class="idRow">
				<div class="idValue" title={revisionId || '-'}>{revisionId || '-'}</div>
			</div>
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
			<div class="outputControls">
				<button class="tabBtn small" on:click={addApiOutput}>+ Add output</button>
			</div>
			{#if outputs.length === 0}
				<div class="muted">No output ports</div>
			{:else}
				{#each outputs as port, index (port.name)}
					<div class="outputEditorRow">
						<input
							placeholder="output name"
							value={String(port.name ?? '')}
							on:input={(e) => updateApiOutput(index, { name: (e.currentTarget as HTMLInputElement).value })}
						/>
						<select
							value={String(port.portType ?? 'json')}
							on:change={(e) => updateApiOutput(index, { portType: (e.currentTarget as HTMLSelectElement).value as PortType })}
						>
							{#each PORT_TYPE_OPTIONS as p}
								<option value={p}>{p}</option>
							{/each}
						</select>
						<label class="requiredToggle">
							<input
								type="checkbox"
								checked={Boolean(port.required ?? true)}
								on:change={(e) => updateApiOutput(index, { required: (e.currentTarget as HTMLInputElement).checked })}
							/>
							required
						</label>
						<button class="tabBtn small danger" title="Remove output" on:click={() => removeApiOutput(index)}>-</button>
					</div>
					<div class="outputSchemaRow">
						<div class="schemaType">
							<span class="k">typedSchema.type</span>
							<select
								value={String(port.typedSchema?.type ?? 'unknown')}
								on:change={(e) =>
									updateApiOutput(index, {
										typedSchema: {
											...(port.typedSchema ?? { fields: [] }),
											type: (e.currentTarget as HTMLSelectElement).value as (typeof TYPED_TYPE_OPTIONS)[number]
										}
									})}
							>
								{#each TYPED_TYPE_OPTIONS as t}
									<option value={t}>{t}</option>
								{/each}
							</select>
						</div>
						<div class="schemaFields">
							<span class="k">typedSchema.fields (JSON array)</span>
							<textarea
								rows="3"
								value={JSON.stringify(port.typedSchema?.fields ?? [], null, 2)}
								on:change={(e) => onApiOutputFieldsJsonChange(index, (e.currentTarget as HTMLTextAreaElement).value)}
							></textarea>
							{#if outputFieldsJsonErrors[index]}
								<div class="bindingErr">Invalid fields JSON: {outputFieldsJsonErrors[index]}</div>
							{/if}
						</div>
					</div>
				{/each}
			{/if}
		</div>
	</div>
	<div class="muted">Inputs are derived from selected revision API; outputs are editable for authoring.</div>
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
	<div class="bindingOutputs">
		<div class="groupTitle">outputs</div>
		{#if outputs.length === 0}
			<div class="muted">No declared API outputs</div>
		{:else}
			{#each outputs as outPort (outPort.name)}
				<div class="bindingRow">
					<div class="mono">{outPort.name}</div>
					<select
						value={String(outputBindings?.[outPort.name]?.nodeId ?? '')}
						on:change={(e) => onOutputBindingNodeIdChange(outPort.name, (e.currentTarget as HTMLSelectElement).value)}
					>
						<option value="">internal node</option>
						{#if String(outputBindings?.[outPort.name]?.nodeId ?? '').trim() && !internalNodeOptions.some((n) => n.id === String(outputBindings?.[outPort.name]?.nodeId ?? '').trim())}
							<option value={String(outputBindings?.[outPort.name]?.nodeId ?? '').trim()}>
								missing: {String(outputBindings?.[outPort.name]?.nodeId ?? '').trim()}
							</option>
						{/if}
						{#each internalNodeOptions as opt (opt.id)}
							<option value={opt.id}>{opt.label}</option>
						{/each}
					</select>
					<select
						value={String(outputBindings?.[outPort.name]?.artifact ?? 'current')}
						on:change={(e) => onOutputBindingArtifactChange(outPort.name, (e.currentTarget as HTMLSelectElement).value)}
					>
						<option value="current">current</option>
						<option value="last">last</option>
					</select>
				</div>
				{#if !String(outputBindings?.[outPort.name]?.nodeId ?? '').trim()}
					<div class="bindingErr">bindings.outputs.{outPort.name}.nodeId is required</div>
				{/if}
			{/each}
		{/if}
		{#if loadingRevisionDetail}
			<div class="muted">Loading internal nodes...</div>
		{:else if !loadingRevisionDetail && internalNodeOptions.length === 0}
			<div class="muted">No internal nodes found for selected revision.</div>
		{/if}
	</div>
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

	.rowNoLabel {
		grid-template-columns: minmax(0, 1fr);
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

	.idRow {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.idValue {
		flex: 1 1 auto;
		min-width: 0;
		height: 34px;
		display: flex;
		align-items: center;
		padding: 0 10px;
		border: 1px solid #283044;
		border-radius: 8px;
		background: rgba(9, 14, 26, 0.35);
		font-size: 13px;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.small {
		padding: 6px 10px;
		font-size: 12px;
		white-space: nowrap;
	}

	.danger {
		border-color: rgba(239, 68, 68, 0.45);
		background: rgba(239, 68, 68, 0.12);
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
		overflow-x: hidden;
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

	.outputControls {
		display: flex;
		justify-content: flex-end;
		margin-bottom: 6px;
	}

	.outputEditorRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) minmax(0, 96px) auto auto;
		gap: 8px;
		align-items: center;
		margin-bottom: 6px;
	}

	.outputSchemaRow {
		display: grid;
		grid-template-columns: minmax(0, 140px) minmax(0, 1fr);
		gap: 8px;
		margin-bottom: 8px;
	}

	.schemaType,
	.schemaFields {
		display: grid;
		gap: 4px;
	}

	.requiredToggle {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		font-size: 12px;
		opacity: 0.9;
	}

	.bindingOutputs {
		margin-top: 8px;
		border: 1px solid #1f2430;
		border-radius: 8px;
		padding: 8px;
		overflow-x: hidden;
	}

	.bindingRow {
		display: grid;
		grid-template-columns: 1fr minmax(0, 1.4fr) 120px;
		gap: 8px;
		align-items: center;
		margin-bottom: 6px;
	}

	@media (max-width: 760px) {
		.outputEditorRow {
			grid-template-columns: minmax(0, 1fr) minmax(0, 110px);
		}

		.outputEditorRow > input {
			grid-column: 1 / -1;
		}

		.requiredToggle {
			grid-column: 1;
			justify-self: start;
		}

		.outputEditorRow .danger {
			grid-column: 2;
			justify-self: end;
		}

		.outputSchemaRow {
			grid-template-columns: minmax(0, 1fr);
		}

		.bindingRow {
			grid-template-columns: minmax(0, 1fr);
		}
	}

	.bindingErr {
		margin: -2px 0 8px 0;
		font-size: 12px;
		color: #ff7b72;
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
