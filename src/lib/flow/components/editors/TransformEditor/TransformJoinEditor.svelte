<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformJoinParams } from '$lib/flow/schema/transform';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';
	import type { InputSchemaView } from './inputSchema';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import {
		canCommitJoin,
		joinMismatchColumnsFromError,
		normalizeJoinParams,
		resolveJoinNodeColumns,
		shortNodeId,
		supportedJoinModes,
		type JoinClause,
		type JoinHow
	} from './joinModel';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformJoinParams>;
	export let onDraft: (patch: Partial<TransformJoinParams>) => void;
	export let onCommit: (patch: Partial<TransformJoinParams>) => void;
	export let inputSchemas: InputSchemaView[] = [];
	export let nodeError: NodeExecutionError | null = null;

	const joinModes: JoinHow[] = supportedJoinModes();
	const defaultHow: JoinHow = 'inner';
	type LocalClause = {
		leftNodeId: string;
		leftCol: string;
		rightNodeId: string;
		rightCol: string;
		how: JoinHow;
	};

	let clauses: LocalClause[] = [];
	let typedDraft = '';
	let lastNodeId = '';
	let lastParamsSignature = '';
	let suppressParamSync = false;

	$: void selectedNode?.id;
	$: normalizedParams = normalizeJoinParams(params);
	$: paramsSignature = JSON.stringify(normalizedParams);
	$: nodeColumns = resolveJoinNodeColumns(inputSchemas);
	$: hasKnownSchema = nodeColumns.length > 0;
	$: missingFromError = joinMismatchColumnsFromError(nodeError);
	$: completedClauses = clauses
		.filter((c) => c.leftNodeId && c.leftCol && c.rightNodeId && c.rightCol)
		.map((c) => ({
			leftNodeId: c.leftNodeId,
			leftCol: c.leftCol,
			rightNodeId: c.rightNodeId,
			rightCol: c.rightCol,
			how: c.how
		}));
	$: canCommit = canCommitJoin(completedClauses as JoinClause[]);

	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		clauses = normalizedParams.clauses.map((c) => ({ ...c }));
		lastParamsSignature = paramsSignature;
	}
	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && paramsSignature !== lastParamsSignature) {
		clauses = normalizedParams.clauses.map((c) => ({ ...c }));
		lastParamsSignature = paramsSignature;
	}

	function markLocalEdit() {
		suppressParamSync = true;
		queueMicrotask(() => {
			suppressParamSync = false;
		});
	}

	function commitClauses(next: LocalClause[]): void {
		const nextLocal = next.map((c) => ({ ...c }));
		const merged = normalizeJoinParams({
			clauses: nextLocal
				.filter((c) => c.leftNodeId && c.leftCol && c.rightNodeId && c.rightCol)
				.map((c) => ({
					leftNodeId: c.leftNodeId,
					leftCol: c.leftCol,
					rightNodeId: c.rightNodeId,
					rightCol: c.rightCol,
					how: c.how
				}))
		});
		markLocalEdit();
		clauses = nextLocal;
		onDraft(merged);
		onCommit(merged);
	}

	type SlotRef = { clauseIndex: number; side: 'left' | 'right' };

	function nextOpenSlot(): SlotRef {
		for (let i = 0; i < clauses.length; i += 1) {
			const c = clauses[i];
			if (!c.leftNodeId || !c.leftCol) return { clauseIndex: i, side: 'left' };
			if (!c.rightNodeId || !c.rightCol) return { clauseIndex: i, side: 'right' };
		}
		const next = [...clauses, { leftNodeId: '', leftCol: '', rightNodeId: '', rightCol: '', how: defaultHow }];
		commitClauses(next);
		return { clauseIndex: next.length - 1, side: 'left' };
	}

	function fillFromPick(nodeId: string, col: string): void {
		const node = String(nodeId ?? '').trim();
		const column = String(col ?? '').trim();
		if (!node || !column) return;
		const slot = nextOpenSlot();
		const next = clauses.map((c) => ({ ...c }));
		const clause = next[slot.clauseIndex];
		if (!clause.how) clause.how = defaultHow;
		if (slot.side === 'left') {
			clause.leftNodeId = node;
			clause.leftCol = column;
		} else {
			clause.rightNodeId = node;
			clause.rightCol = column;
		}
		commitClauses(next);
	}

	function parseTypedQualified(value: string): { nodeId: string; col: string } | null {
		const raw = String(value ?? '').trim();
		const idx = raw.indexOf('.');
		if (idx <= 0 || idx >= raw.length - 1) return null;
		const nodeToken = raw.slice(0, idx).trim();
		const col = raw.slice(idx + 1).trim();
		if (!nodeToken || !col) return null;
		// Accept full node id or short id token.
		const resolved = nodeColumns.find(
			(n) =>
				n.nodeId === nodeToken ||
				n.shortId === nodeToken ||
				n.label === nodeToken ||
				n.displayName === nodeToken
		);
		if (resolved) return { nodeId: resolved.nodeId, col };
		return { nodeId: nodeToken, col };
	}

	function addTyped(): void {
		const parsed = parseTypedQualified(typedDraft);
		if (!parsed) return;
		fillFromPick(parsed.nodeId, parsed.col);
		typedDraft = '';
	}

	function removeClause(index: number): void {
		commitClauses(clauses.filter((_, i) => i !== index));
	}

	function setClauseHow(index: number, how: JoinHow): void {
		const next = clauses.map((c, i) => (i === index ? { ...c, how } : c));
		commitClauses(next);
	}

	function displayQualified(nodeId: string, col: string): string {
		if (!nodeId || !col) return '(empty)';
		const row = nodeColumns.find((n) => n.nodeId === nodeId);
		const left = row?.displayName ?? shortNodeId(nodeId);
		return `${left}.${col}`;
	}
</script>

<Section title="Join">
	<div class="hint">Click node columns to fill slots in order: left, right, then next clause. Clause rows auto-create.</div>

		{#if !hasKnownSchema}
			<div class="typedRow">
				<Input
					value={typedDraft}
					placeholder="type nodeName.column"
					onInput={(event) => (typedDraft = (event.currentTarget as HTMLInputElement).value)}
					onKeydown={(event) => {
						if ((event as KeyboardEvent).key !== 'Enter') return;
					event.preventDefault();
					addTyped();
				}}
			/>
			<button class="small" type="button" on:click={addTyped}>+</button>
		</div>
	{/if}

	<div class="clausesSticky">
		<div class="clausesTitle">Clauses</div>
		<div class="clausesList">
			{#if clauses.length === 0}
				<div class="clauseRow placeholder">
					<div class="slot">(empty)</div>
					<select value={defaultHow} disabled>
						{#each joinModes as mode}
							<option value={mode}>{mode}</option>
						{/each}
					</select>
					<div class="slot">(empty)</div>
					<button class="small danger" type="button" disabled>Remove</button>
				</div>
			{:else}
				{#each clauses as clause, index}
					<div class="clauseRow">
						<div class="slot">{displayQualified(clause.leftNodeId, clause.leftCol)}</div>
						<select value={clause.how ?? defaultHow} on:change={(event) => setClauseHow(index, (event.currentTarget as HTMLSelectElement).value as JoinHow)}>
							{#each joinModes as mode}
								<option value={mode}>{mode}</option>
							{/each}
						</select>
						<div class="slot">{displayQualified(clause.rightNodeId, clause.rightCol)}</div>
						<button class="small danger" type="button" on:click={() => removeClause(index)}>Remove</button>
					</div>
				{/each}
			{/if}
		</div>
	</div>

	<div class="nodeTable">
		<div class="tableHead">Node</div>
		<div class="tableHead">Columns</div>
		{#if nodeColumns.length === 0}
			<div class="tableCell muted">No connected artifacts</div>
			<div class="tableCell muted">Run upstream to populate columns</div>
		{:else}
			{#each nodeColumns as row}
				<div class="tableCell nodeCell">{row.displayName}</div>
				<div class="tableCell colsCell">
					{#if row.columns.length === 0}
						<span class="muted">No columns</span>
					{:else}
						{#each row.columns as col}
							<button class="chipBtn" type="button" on:click={() => fillFromPick(row.nodeId, col)}>
								{row.displayName}.{col}
							</button>
						{/each}
					{/if}
				</div>
			{/each}
		{/if}
	</div>

	{#if !canCommit}
		<div class="warn">Join requires at least one complete clause.</div>
	{/if}
	{#if missingFromError.length > 0}
		<div class="warn">Runtime mismatch: {missingFromError.join(', ')}</div>
	{/if}
</Section>

<style>
	.hint,
	.warn {
		font-size: 12px;
		margin-top: 6px;
	}

	.hint {
		opacity: 0.75;
	}

	.warn {
		color: #fca5a5;
	}

	.typedRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 8px;
		align-items: center;
		margin-top: 8px;
	}

	.nodeTable {
		display: grid;
		grid-template-columns: minmax(120px, 0.35fr) minmax(0, 1fr);
		gap: 6px 8px;
		margin-top: 10px;
	}

	.tableHead {
		font-size: 12px;
		font-weight: 700;
		opacity: 0.9;
	}

	.tableCell {
		border: 1px solid rgba(255, 255, 255, 0.14);
		border-radius: 10px;
		padding: 8px;
		min-height: 38px;
	}

	.nodeCell {
		font-size: 12px;
		font-weight: 700;
	}

	.colsCell {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
	}

	.muted {
		font-size: 12px;
		opacity: 0.7;
	}

	.chipBtn {
		padding: 4px 8px;
		font-size: 12px;
		border-radius: 8px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		cursor: pointer;
	}

	.clausesTitle {
		margin-top: 10px;
		font-size: 12px;
		font-weight: 700;
	}

	.clausesSticky {
		position: sticky;
		top: 0;
		z-index: 3;
		background: var(--ni-card, #0f1724);
		border: 1px solid rgba(255, 255, 255, 0.14);
		border-radius: 10px;
		padding: 8px;
		margin-top: 10px;
	}

	.clausesSticky .clausesTitle {
		margin-top: 0;
	}

	.clausesList {
		margin-top: 6px;
		display: grid;
		gap: 8px;
	}

	.clauseRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr) auto;
		gap: 8px;
		align-items: center;
	}

	.clauseRow.placeholder {
		opacity: 0.75;
	}

	.slot {
		border: 1px solid rgba(255, 255, 255, 0.14);
		border-radius: 8px;
		padding: 8px;
		font-size: 12px;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	button.small {
		padding: 6px 10px;
		font-size: 12px;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.06);
		color: inherit;
		cursor: pointer;
	}

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
	}

	@media (max-width: 880px) {
		.nodeTable {
			grid-template-columns: 1fr;
		}

		.clauseRow {
			grid-template-columns: 1fr;
		}
	}
</style>
