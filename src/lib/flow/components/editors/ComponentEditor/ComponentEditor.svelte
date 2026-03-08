<script lang="ts">
	import KeyValueEditor from '$lib/flow/components/KeyValueEditor.svelte';
	import ToolbarMenu from '$lib/flow/components/ToolbarMenu.svelte';
	import type { ToolbarMenuItem } from '$lib/flow/components/toolbarMenu';
	import { graphStore } from '$lib/flow/store/graphStore';
	import type { ComponentNodeData } from '$lib/flow/types';
	import type {
		ComponentApiPort,
		ComponentCatalogItem,
		ComponentRevisionSummary,
		ComponentTypedField
	} from '$lib/flow/client/components';
	import type { PortType } from '$lib/flow/types';
	import {
		applyDerivedOutputPortTypes,
		syncOutputBindings,
		validateComponentOutputs
	} from './validation';

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
	let internalNodeMetaById: Record<string, { outPortType?: PortType }> = {};

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
	const STRUCTURED_OUTPUT_TYPES = new Set<PortType>(['table', 'json']);
	type TypedFieldType = ComponentTypedField['type'];
	let outputFieldsJsonErrors: Record<number, string> = {};
	let outputAdvancedOpen: Record<number, boolean> = {};
	let outputFieldsEditorMode: Record<number, 'structured' | 'json'> = {};
	let outputNameDraftByIndex: Record<number, string> = {};
	let outputSyncSignature = '';
	let outputCanonicalSignature = '';
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
	$: outputValidation = validateComponentOutputs(outputs, outputBindings);
	$: outputValidationSummary = outputValidation.hasErrors
		? `${Object.keys(outputValidation.outputErrors).length} output issue(s), ${Object.keys(outputValidation.bindingErrors).length} binding issue(s)`
		: '';

	$: {
		const synced = syncOutputBindings(
			outputs,
			outputBindings,
			internalNodeOptions.map((n) => String(n.id ?? '').trim()).filter((id) => id.length > 0)
		);
		if (synced.changed) {
			const nextSignature = JSON.stringify(synced.next);
			if (nextSignature !== outputSyncSignature) {
				outputSyncSignature = nextSignature;
				onDraft({
					bindings: {
						inputs: { ...(bindings?.inputs ?? {}) },
						config: { ...(bindings?.config ?? {}) },
						outputs: synced.next
					}
				});
			}
		}
	}

	$: {
		const derivedOutputs = applyDerivedOutputPortTypes(
			outputs as ComponentApiPort[],
			outputBindings,
			internalNodeMetaById
		);
		const canonicalOutputs = derivedOutputs.map((out) => canonicalizeOutputPort(out as ComponentApiPort));
		const nextSignature = JSON.stringify(canonicalOutputs);
		const currentSignature = JSON.stringify(outputs ?? []);
		if (nextSignature !== currentSignature && nextSignature !== outputCanonicalSignature) {
			outputCanonicalSignature = nextSignature;
			draftApiOutputs(canonicalOutputs);
		}
	}

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
		internalNodeMetaById = {};
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
			internalNodeMetaById = {};
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
			internalNodeMetaById = {};
			lastRevisionDetailKey = '';
			return;
		}
		const detail = (res as any).detail ?? {};
		const graph = (detail?.definition?.graph ?? {}) as { nodes?: any[] };
		const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
		const nextMeta: Record<string, { outPortType?: PortType }> = {};
		internalNodeOptions = nodes
			.map((n) => {
				const id = String(n?.id ?? '').trim();
				if (!id) return null;
				const kind = String(n?.data?.kind ?? 'node').trim();
				const name = String(n?.data?.label ?? id).trim() || id;
				const outPortRaw = String(n?.data?.ports?.out ?? '').trim().toLowerCase();
				if (PORT_TYPE_OPTIONS.includes(outPortRaw as PortType)) {
					nextMeta[id] = { outPortType: outPortRaw as PortType };
				}
				return {
					id,
					label: `${kind}: ${name}`
				};
			})
			.filter((x): x is { id: string; label: string } => Boolean(x))
			.sort((a, b) => a.label.localeCompare(b.label));
		internalNodeMetaById = nextMeta;
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
		const canonical = nextOutputs.map((out) => canonicalizeOutputPort(out as ComponentApiPort));
		onDraft({
			api: {
				inputs: [...inputs],
				outputs: canonical
			}
		});
	}

	function updateApiOutput(
		index: number,
		patch: Partial<ComponentApiPort>
	): void {
		const previousName = String(outputs[index]?.name ?? '').trim();
		const next = outputs.map((out, i) =>
			i === index
				? canonicalizeOutputPort({ ...(out as ComponentApiPort), ...(patch as ComponentApiPort) })
				: canonicalizeOutputPort(out as ComponentApiPort)
		);
		draftApiOutputs(next as ComponentApiPort[]);
		const nextName = String(next[index]?.name ?? '').trim();
		if (previousName && nextName && previousName !== nextName) {
			const nextBindings = { ...(bindings?.outputs ?? {}) } as Record<
				string,
				{ nodeId?: string; artifact?: 'current' | 'last' }
			>;
			const moved = nextBindings[previousName];
			if (moved) {
				delete nextBindings[previousName];
				nextBindings[nextName] = moved;
				onDraft({
					bindings: {
						inputs: { ...(bindings?.inputs ?? {}) },
						config: { ...(bindings?.config ?? {}) },
						outputs: nextBindings
					}
				});
			}
		}
	}

	function getOutputNameDraft(index: number, currentName: string): string {
		return Object.prototype.hasOwnProperty.call(outputNameDraftByIndex, index)
			? String(outputNameDraftByIndex[index] ?? '')
			: currentName;
	}

	function setOutputNameDraft(index: number, value: string): void {
		outputNameDraftByIndex = {
			...outputNameDraftByIndex,
			[index]: value
		};
	}

	function cancelOutputNameDraft(index: number): void {
		if (!Object.prototype.hasOwnProperty.call(outputNameDraftByIndex, index)) return;
		const next = { ...outputNameDraftByIndex };
		delete next[index];
		outputNameDraftByIndex = next;
	}

	function commitOutputNameDraft(index: number): void {
		if (!Object.prototype.hasOwnProperty.call(outputNameDraftByIndex, index)) return;
		const nextName = String(outputNameDraftByIndex[index] ?? '');
		const currentName = String(outputs[index]?.name ?? '');
		if (nextName !== currentName) {
			updateApiOutput(index, { name: nextName });
		}
		cancelOutputNameDraft(index);
	}

	function addApiOutput(): void {
		errorMessage = '';
		outputNameDraftByIndex = {};
		const nextName = outputs.length === 0 ? 'default' : `out_${outputs.length + 1}`;
		const fallbackNodeId = String(internalNodeOptions[0]?.id ?? '').trim() || undefined;
		const initialPortType = normalizePortType(
			(fallbackNodeId ? internalNodeMetaById[fallbackNodeId]?.outPortType : undefined) ?? 'json'
		);
		const next = [
			...outputs,
			{
				name: nextName,
				portType: initialPortType,
				required: true,
				typedSchema: { type: initialPortType, fields: [] }
			}
		];
		draftApiOutputs(next as ComponentApiPort[]);
		onDraft({
			bindings: {
				inputs: { ...(bindings?.inputs ?? {}) },
				config: { ...(bindings?.config ?? {}) },
				outputs: {
					...(bindings?.outputs ?? {}),
					[nextName]: {
						nodeId: fallbackNodeId,
						artifact: 'current'
					}
				}
			}
		});
	}

	function isOutputBindingCurrent(name: string): boolean {
		return String(outputBindings?.[name]?.artifact ?? 'current') !== 'last';
	}

	function toggleOutputBindingArtifact(name: string): void {
		onOutputBindingArtifactChange(name, isOutputBindingCurrent(name) ? 'last' : 'current');
	}

	function resetOutputSchema(index: number): void {
		const output = outputs[index];
		if (!output) return;
		const portType = normalizePortType(output.portType);
		updateApiOutput(index, {
			typedSchema: {
				type: portType as TypedFieldType,
				fields: []
			}
		});
	}

	function removeApiOutput(index: number): void {
		outputNameDraftByIndex = {};
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
		const nextAdvancedOpen = { ...outputAdvancedOpen };
		delete nextAdvancedOpen[index];
		outputAdvancedOpen = nextAdvancedOpen;
		const nextEditorMode = { ...outputFieldsEditorMode };
		delete nextEditorMode[index];
		outputFieldsEditorMode = nextEditorMode;
	}

	function isOutputAdvancedOpen(index: number): boolean {
		return Boolean(outputAdvancedOpen[index]);
	}

	function toggleOutputAdvanced(index: number): void {
		outputAdvancedOpen = {
			...outputAdvancedOpen,
			[index]: !isOutputAdvancedOpen(index)
		};
	}

	function getFieldsEditorMode(index: number): 'structured' | 'json' {
		return outputFieldsEditorMode[index] ?? 'structured';
	}

	function setFieldsEditorMode(index: number, mode: 'structured' | 'json'): void {
		outputFieldsEditorMode = {
			...outputFieldsEditorMode,
			[index]: mode
		};
	}

	function normalizeTypedFieldType(value: unknown): TypedFieldType {
		const t = String(value ?? '').trim();
		return (TYPED_TYPE_OPTIONS as readonly string[]).includes(t) ? (t as TypedFieldType) : 'unknown';
	}

	function normalizePortType(value: unknown): PortType {
		const v = String(value ?? '').trim().toLowerCase();
		return PORT_TYPE_OPTIONS.includes(v as PortType) ? (v as PortType) : 'json';
	}

	function shouldShowStructuredFieldsEditor(portType: unknown): boolean {
		return STRUCTURED_OUTPUT_TYPES.has(normalizePortType(portType));
	}

	function canonicalizeFieldsForPortType(
		portType: PortType,
		fieldsRaw: unknown
	): ComponentTypedField[] {
		if (!shouldShowStructuredFieldsEditor(portType)) return [];
		if (!Array.isArray(fieldsRaw)) return [];
		return fieldsRaw
			.filter((f): f is Record<string, unknown> => Boolean(f) && typeof f === 'object')
			.map((f) => ({
				name: String(f.name ?? '').trim(),
				type: normalizeTypedFieldType(f.type),
				nativeType: f.nativeType == null ? undefined : String(f.nativeType),
				nullable: Boolean(f.nullable ?? false)
			}));
	}

	function canonicalizeOutputPort(port: ComponentApiPort): ComponentApiPort {
		const normalizedPortType = normalizePortType((port as any)?.portType);
		const typedSchema = (port as any)?.typedSchema ?? {};
		return {
			...port,
			portType: normalizedPortType,
			required: Boolean((port as any)?.required ?? true),
			typedSchema: {
				type: normalizedPortType as TypedFieldType,
				fields: canonicalizeFieldsForPortType(normalizedPortType, (typedSchema as any)?.fields)
			}
		};
	}

	function getOutputFields(index: number): ComponentTypedField[] {
		const portType = normalizePortType(outputs[index]?.portType ?? 'json');
		const fields = outputs[index]?.typedSchema?.fields;
		return canonicalizeFieldsForPortType(portType, fields);
	}

	function updateOutputField(
		outputIndex: number,
		fieldIndex: number,
		patch: Partial<ComponentTypedField>
	): void {
		const portType = normalizePortType(outputs[outputIndex]?.portType);
		const fields = getOutputFields(outputIndex);
		const nextFields = fields.map((field, i) => (i === fieldIndex ? { ...field, ...patch } : field));
		updateApiOutput(outputIndex, {
			typedSchema: {
				type: portType as TypedFieldType,
				fields: nextFields
			}
		});
	}

	function addOutputField(outputIndex: number): void {
		const portType = normalizePortType(outputs[outputIndex]?.portType);
		if (!shouldShowStructuredFieldsEditor(portType)) return;
		const fields = getOutputFields(outputIndex);
		const newField: ComponentTypedField = {
			name: `field_${fields.length + 1}`,
			type: 'unknown',
			nullable: false
		};
		const nextFields = [...fields, newField];
		updateApiOutput(outputIndex, {
			typedSchema: {
				type: portType as TypedFieldType,
				fields: nextFields
			}
		});
	}

	function removeOutputField(outputIndex: number, fieldIndex: number): void {
		const portType = normalizePortType(outputs[outputIndex]?.portType);
		const fields = getOutputFields(outputIndex);
		const nextFields = fields.filter((_, i) => i !== fieldIndex);
		updateApiOutput(outputIndex, {
			typedSchema: {
				type: portType as TypedFieldType,
				fields: nextFields
			}
		});
	}

	function onApiOutputFieldsJsonChange(index: number, raw: string): void {
		try {
			let parsed: any = [];
			const trimmed = raw.trim();
			if (!trimmed) {
				parsed = [];
			} else {
				try {
					parsed = JSON.parse(trimmed);
				} catch {
					// Legacy shorthand support: [field_a, field_b]
					const m = trimmed.match(/^\[\s*([^\]]*)\s*\]$/);
					if (!m) throw new Error('typedSchema.fields must be valid JSON array');
					const body = String(m[1] ?? '').trim();
					const names = body
						.split(',')
						.map((s) => s.trim())
						.filter((s) => s.length > 0)
						.map((s) => s.replace(/^['"]|['"]$/g, ''));
					parsed = names.map((name) => ({ name, type: 'unknown' }));
				}
			}
			if (!Array.isArray(parsed)) {
				outputFieldsJsonErrors = {
					...outputFieldsJsonErrors,
					[index]: 'typedSchema.fields must be a JSON array'
				};
				return;
			}
			const normalized: ComponentTypedField[] = parsed.map((f) => ({
				name: String((f as any)?.name ?? '').trim(),
				type: normalizeTypedFieldType((f as any)?.type),
				nativeType:
					(f as any)?.nativeType == null ? undefined : String((f as any).nativeType),
				nullable: Boolean((f as any)?.nullable ?? false)
			}));
			const portType = normalizePortType(outputs[index]?.portType);
			updateApiOutput(index, {
				typedSchema: {
					type: portType as TypedFieldType,
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
				<button
					class="tabBtn small"
					type="button"
					on:click={addApiOutput}
					disabled={loadingRevisionDetail}
					title={loadingRevisionDetail ? 'Loading revision detail...' : undefined}
				>
					+ Add output
				</button>
			</div>
			{#if outputValidation.hasErrors}
				<div class="validationSummary">Resolve output/binding issues before Accept: {outputValidationSummary}</div>
			{/if}
			{#if outputs.length === 0}
				<div class="muted">No output ports</div>
			{:else}
				{#each outputs as port, index (`${index}:${port.name}`)}
					<div class="outputCard">
						<div class="outputEditorRow outputEditorRowPrimary">
							<input
								placeholder="output name"
								value={getOutputNameDraft(index, String(port.name ?? ''))}
								on:input={(e) =>
									setOutputNameDraft(index, (e.currentTarget as HTMLInputElement).value)}
								on:blur={() => commitOutputNameDraft(index)}
								on:keydown={(e) => {
									const key = (e as KeyboardEvent).key;
									if (key === 'Enter') {
										e.preventDefault();
										commitOutputNameDraft(index);
										return;
									}
									if (key === 'Escape') {
										e.preventDefault();
										cancelOutputNameDraft(index);
									}
								}}
							/>
							<div class="readonlyField outputTypeReadonly" title="Derived from bound internal node">
								{String(port.portType ?? 'json')}
							</div>
							<div class="outputActions">
								<button class="tabBtn small danger" type="button" title="Remove output" on:click={() => removeApiOutput(index)}>-</button>
							</div>
						</div>
						<div class="outputEditorRow outputEditorRowSecondary">
							<select
								value={String(outputBindings?.[port.name]?.nodeId ?? '')}
								on:change={(e) => onOutputBindingNodeIdChange(port.name, (e.currentTarget as HTMLSelectElement).value)}
							>
								<option value="">internal node</option>
								{#if String(outputBindings?.[port.name]?.nodeId ?? '').trim() && !internalNodeOptions.some((n) => n.id === String(outputBindings?.[port.name]?.nodeId ?? '').trim())}
									<option value={String(outputBindings?.[port.name]?.nodeId ?? '').trim()}>
										missing: {String(outputBindings?.[port.name]?.nodeId ?? '').trim()}
									</option>
								{/if}
								{#each internalNodeOptions as opt (opt.id)}
									<option value={opt.id}>{opt.label}</option>
								{/each}
							</select>
							<button
								class={`tabBtn small toggleMode ${isOutputBindingCurrent(port.name) ? 'active' : ''}`}
								type="button"
								title="Toggle current/last artifact"
								on:click={() => toggleOutputBindingArtifact(port.name)}
							>
								{isOutputBindingCurrent(port.name) ? 'current' : 'last'}
							</button>
							<label class="requiredToggle">
								<input
									type="checkbox"
									checked={Boolean(port.required ?? true)}
									on:change={(e) => updateApiOutput(index, { required: (e.currentTarget as HTMLInputElement).checked })}
								/>
								req
							</label>
							<div class="outputActions">
								<button class="tabBtn small" type="button" on:click|stopPropagation={() => toggleOutputAdvanced(index)}>
									Adv {isOutputAdvancedOpen(index) ? '▴' : '▾'}
								</button>
							</div>
						</div>
						{#if !isOutputAdvancedOpen(index) && outputValidation.outputErrors[index]?.length}
							<div class="schemaIssueBadge">Schema issue</div>
						{/if}
						{#if isOutputAdvancedOpen(index)}
							<div class="outputSchemaRow">
								<div class="schemaType">
									<span class="k">typedSchema.type</span>
									<div class="readonlyField">{String(port.portType ?? 'json')}</div>
								</div>
								<div class="schemaFields">
									<div class="schemaFieldsHeader">
										<span class="k">typedSchema.fields</span>
										<div class="schemaFieldActions">
											{#if shouldShowStructuredFieldsEditor(port.portType)}
												<button
													class="tabBtn small"
													type="button"
													on:click={() => setFieldsEditorMode(index, getFieldsEditorMode(index) === 'structured' ? 'json' : 'structured')}
												>
													{getFieldsEditorMode(index) === 'structured' ? 'JSON' : 'List'}
												</button>
												{#if getFieldsEditorMode(index) === 'structured'}
													<button class="tabBtn small" type="button" on:click={() => addOutputField(index)}>+ field</button>
												{/if}
											{/if}
											<button class="tabBtn small" type="button" on:click={() => resetOutputSchema(index)}>Reset</button>
										</div>
									</div>
									{#if shouldShowStructuredFieldsEditor(port.portType)}
										{#if getFieldsEditorMode(index) === 'structured'}
											{#if getOutputFields(index).length === 0}
												<div class="muted">No fields</div>
											{:else}
												{#each getOutputFields(index) as field, fieldIndex (`${index}:${fieldIndex}:${field.name}`)}
													<div class="fieldRow">
														<input
															placeholder="field name"
															value={String(field.name ?? '')}
															on:input={(e) => updateOutputField(index, fieldIndex, { name: (e.currentTarget as HTMLInputElement).value })}
														/>
														<select
															value={String(field.type ?? 'unknown')}
															on:change={(e) =>
																updateOutputField(index, fieldIndex, {
																	type: normalizeTypedFieldType(
																		(e.currentTarget as HTMLSelectElement).value
																	)
																})}
														>
															{#each TYPED_TYPE_OPTIONS as t}
																<option value={t}>{t}</option>
															{/each}
														</select>
														<label class="requiredToggle">
															<input
																type="checkbox"
																checked={Boolean(field.nullable ?? false)}
																on:change={(e) => updateOutputField(index, fieldIndex, { nullable: (e.currentTarget as HTMLInputElement).checked })}
															/>
															nullable
														</label>
														<button class="tabBtn small danger" type="button" title="Remove field" on:click={() => removeOutputField(index, fieldIndex)}>-</button>
													</div>
												{/each}
											{/if}
										{:else}
											<textarea
												rows="3"
												value={JSON.stringify(port.typedSchema?.fields ?? [], null, 2)}
												on:change={(e) => onApiOutputFieldsJsonChange(index, (e.currentTarget as HTMLTextAreaElement).value)}
											></textarea>
											{#if outputFieldsJsonErrors[index]}
												<div class="bindingErr">Invalid fields JSON: {outputFieldsJsonErrors[index]}</div>
											{/if}
										{/if}
									{:else}
										<div class="muted">No schema fields for {String(port.portType ?? 'json')} outputs.</div>
									{/if}
								</div>
							</div>
							{#if outputValidation.outputErrors[index]?.length}
								<div class="bindingErr">
									{#each outputValidation.outputErrors[index] as issue}
										<div>{issue}</div>
									{/each}
								</div>
							{/if}
						{/if}
						{#if !String(outputBindings?.[port.name]?.nodeId ?? '').trim()}
							<div class="bindingErr">bindings.outputs.{port.name}.nodeId is required</div>
						{/if}
						{#if outputValidation.bindingErrors[port.name]?.length}
							<div class="bindingErr">
								{#each outputValidation.bindingErrors[port.name] as issue}
									<div>{issue}</div>
								{/each}
							</div>
						{/if}
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
	<div class="muted">Output bindings and mode are configured in API Contract &gt; Outputs.</div>
	{#if loadingRevisionDetail}
		<div class="muted">Loading internal nodes...</div>
	{:else if !loadingRevisionDetail && internalNodeOptions.length === 0}
		<div class="muted">No internal nodes found for selected revision.</div>
	{/if}
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
		grid-template-columns: minmax(0, 1fr) minmax(0, 110px) auto;
		gap: 8px;
		align-items: center;
		margin-bottom: 0;
	}

	.outputEditorRowSecondary {
		grid-template-columns: minmax(0, 1fr) 86px auto auto;
		margin-top: 8px;
	}

	.outputActions {
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}

	.toggleMode {
		min-width: 74px;
		text-transform: lowercase;
	}

	.toggleMode.active {
		border-color: rgba(59, 130, 246, 0.55);
		background: rgba(59, 130, 246, 0.2);
	}

	.outputCard {
		border: 1px solid #1f2430;
		border-radius: 8px;
		padding: 8px;
		margin-bottom: 8px;
	}

	.outputSchemaRow {
		display: grid;
		grid-template-columns: minmax(0, 140px) minmax(0, 1fr);
		gap: 8px;
		margin-top: 8px;
	}

	.schemaType,
	.schemaFields {
		display: grid;
		gap: 4px;
	}

	.readonlyField {
		height: 32px;
		display: flex;
		align-items: center;
		padding: 0 10px;
		border: 1px solid #283044;
		border-radius: 8px;
		background: rgba(9, 14, 26, 0.35);
		font-size: 12px;
	}

	.schemaFieldsHeader {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}

	.schemaFieldActions {
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}

	.fieldRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) minmax(0, 110px) auto auto;
		gap: 8px;
		align-items: center;
	}

	.schemaIssueBadge {
		display: inline-flex;
		align-items: center;
		margin-top: 8px;
		padding: 2px 8px;
		border-radius: 999px;
		border: 1px solid rgba(251, 146, 60, 0.55);
		background: rgba(251, 146, 60, 0.16);
		color: #ffb86b;
		font-size: 11px;
		font-weight: 600;
	}

	.requiredToggle {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		font-size: 12px;
		opacity: 0.9;
	}

	/* Component-editor-local select skin for output/binding choosers */
	.section :global(select) {
		background: var(--color-control-bg);
		color: var(--color-control-text);
		border: 1px solid var(--color-control-border);
		-webkit-text-fill-color: var(--color-control-text);
	}

	.section :global(select option),
	.section :global(select optgroup) {
		background: var(--color-control-option-bg) !important;
		color: var(--color-control-option-text) !important;
	}

	.section :global(select option:checked),
	.section :global(select option:hover) {
		background: var(--color-control-option-active-bg) !important;
		color: var(--color-control-option-text) !important;
	}

	:global(:root[data-theme='dark']) .section :global(select) {
		color-scheme: dark;
	}

	:global(:root[data-theme='light']) .section :global(select) {
		color-scheme: light;
	}

	@media (prefers-color-scheme: dark) {
		:global(:root:not([data-theme])) .section :global(select) {
			color-scheme: dark;
		}
	}

	@media (prefers-color-scheme: light) {
		:global(:root:not([data-theme])) .section :global(select) {
			color-scheme: light;
		}
	}

	@media (max-width: 760px) {
		.outputEditorRow {
			grid-template-columns: minmax(0, 1fr) minmax(0, 110px) auto;
		}

		.outputEditorRowSecondary {
			grid-template-columns: minmax(0, 1fr) 86px;
		}

		.outputEditorRowPrimary > input {
			grid-column: 1 / -1;
		}

		.requiredToggle {
			grid-column: 1;
			justify-self: start;
		}

		.outputActions {
			grid-column: 2 / -1;
			justify-content: flex-end;
		}

		.outputEditorRowSecondary > select:first-child {
			grid-column: 1 / -1;
		}

		.outputSchemaRow {
			grid-template-columns: minmax(0, 1fr);
		}

		.fieldRow {
			grid-template-columns: minmax(0, 1fr) minmax(0, 110px);
		}

		.fieldRow > input {
			grid-column: 1 / -1;
		}

	}

	.bindingErr {
		margin: -2px 0 8px 0;
		font-size: 12px;
		color: #ff7b72;
	}

	.validationSummary {
		margin: 0 0 8px 0;
		font-size: 12px;
		color: #ffb86b;
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
