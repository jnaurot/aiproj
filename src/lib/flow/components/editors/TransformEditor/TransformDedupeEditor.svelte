<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformDedupeParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import {
		canCommitDedupeDraft,
		missingDedupeColumnsFromError,
		normalizeDedupeParams,
		resolveDedupeAvailableColumns
	} from './dedupeModel';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';
	import { SCHEMA_UNAVAILABLE_VALUE, SCHEMA_UNAVAILABLE_LABEL } from '$lib/flow/constants';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformDedupeParams>;
	export let onDraft: (patch: Partial<TransformDedupeParams>) => void;
	export let onCommit: (patch: Partial<TransformDedupeParams>) => void;
	export let inputColumns: string[] = [];
	export let nodeError: NodeExecutionError | null = null;

	const defaults: TransformDedupeParams = {
		allColumns: false,
		by: [SCHEMA_UNAVAILABLE_VALUE]
	};

	let by: string[] = [];
	let useByColumns = false;
	let selectedColumnToAdd = '';
	let lastNodeId = '';
	let replacementByIndex: Record<number, string> = {};
	let stickyAvailableColumns: string[] = [];

	$: void selectedNode?.id;
	$: normalized = normalizeDedupeParams(params);
	$: allColumns = normalized.allColumns;
	$: by = normalized.by;
	$: errorAvailableColumns = Array.isArray(nodeError?.availableColumns)
		? nodeError.availableColumns.map((c) => String(c).trim()).filter(Boolean)
		: [];
	$: resolvedColumns = resolveDedupeAvailableColumns(inputColumns, errorAvailableColumns, by);
	$: if (resolvedColumns.length > 0) stickyAvailableColumns = resolvedColumns;
	$: availableColumns = resolvedColumns.length > 0 ? resolvedColumns : stickyAvailableColumns;
	$: fallbackColumns = uniqueStrings([...inputColumns, ...errorAvailableColumns, ...by]);
	$: missingByColumns = missingDedupeColumnsFromError(nodeError);
	$: isMissingColumnError = missingByColumns.length > 0;
	$: dedupeAll = allColumns;
	$: canCommit = canCommitDedupeDraft(useByColumns, by);

	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		useByColumns = !allColumns;
	}

	$: if (!selectedColumnToAdd || !availableColumns.includes(selectedColumnToAdd)) {
		selectedColumnToAdd =
			availableColumns[0] ??
			(fallbackColumns[0] ?? (useByColumns ? SCHEMA_UNAVAILABLE_VALUE : ''));
	}

	$: {
		const next: Record<number, string> = {};
		for (let i = 0; i < by.length; i += 1) {
			const current = replacementByIndex[i];
			next[i] = availableColumns.includes(current) ? current : (availableColumns[0] ?? '');
		}
		replacementByIndex = next;
	}

	function setDraft(next: Partial<TransformDedupeParams>) {
		const merged = normalizeDedupeParams({ ...normalized, ...next });
		console.log('[dedupe-ui] setDraft', {
			nodeId: selectedNode?.id,
			next,
			merged,
			currentNormalized: normalized,
			useByColumns
		});
		onDraft(merged);
	}

	function setCommit(next: Partial<TransformDedupeParams>) {
		const merged = normalizeDedupeParams({ ...normalized, ...next });
		const mergedUseByColumns = merged.allColumns === false;
		const allowed = canCommitDedupeDraft(mergedUseByColumns, merged.by ?? []);
		console.log('[dedupe-ui] setCommit', {
			nodeId: selectedNode?.id,
			next,
			merged,
			mergedUseByColumns,
			allowed
		});
		if (!allowed) return;
		onCommit(merged);
	}

	function setCommitAllowInvalid(next: Partial<TransformDedupeParams>) {
		const merged = normalizeDedupeParams({ ...normalized, ...next });
		if (merged.allColumns === false) {
			const cleanBy = uniqueStrings((merged.by ?? []).map((v) => String(v).trim()).filter(Boolean));
			merged.by = cleanBy.length === 0 ? [SCHEMA_UNAVAILABLE_VALUE] : cleanBy;
		} else {
			merged.by = [];
		}
		console.log('[dedupe-ui] setCommitAllowInvalid', {
			nodeId: selectedNode?.id,
			next,
			merged,
			useByColumns
		});
		onCommit(merged);
	}

	function addSelectedColumn() {
		const candidate = selectedColumnToAdd.trim();
		if (!candidate) return;
		const base = by.filter((c) => c !== SCHEMA_UNAVAILABLE_VALUE);
		const next = uniqueStrings([...base, candidate]);
		setDraft({ allColumns: false, by: next });
		setCommit({ allColumns: false, by: next });
	}
</script>

<Section title="Deduplicate">
	<div class="hint">
		Removes duplicate rows based on selected columns.
		Enable <b>Deduplicate on all columns</b> to remove rows that are identical across the entire row.
	</div>
	<div class="hint">Keeps: first row (stable order)</div>

	<div class="actions">
		<label class="toggle">
			<Input
				type="checkbox"
				checked={dedupeAll}
				onChange={(event) => {
					const checked = (event.currentTarget as HTMLInputElement).checked;
					console.log('[dedupe-ui] checkbox:onChange', {
						nodeId: selectedNode?.id,
						checked,
						before: { allColumns, by, useByColumns, dedupeAll }
					});
					if (checked) {
						useByColumns = false;
						setDraft({ allColumns: true, by: [] });
						setCommit({ allColumns: true, by: [] });
						return;
					}
					useByColumns = true;
					const seeded =
						by.filter((c) => c !== SCHEMA_UNAVAILABLE_VALUE).length > 0
							? by.filter((c) => c !== SCHEMA_UNAVAILABLE_VALUE)
							: [SCHEMA_UNAVAILABLE_VALUE];
					setDraft({ allColumns: false, by: seeded });
					setCommitAllowInvalid({ allColumns: false, by: seeded });
				}}
			/>
			<span>Deduplicate on all columns</span>
		</label>

		<button
			class="small ghost"
			type="button"
			on:click={() => {
				useByColumns = true;
				onDraft(defaults);
				onCommit(defaults);
			}}
		>
			Reset
		</button>
	</div>

	{#if useByColumns}
		<Field label="columns">
			<div class="addRow">
				<select bind:value={selectedColumnToAdd}>
					{#if availableColumns.length === 0}
						{#if fallbackColumns.length === 0}
							<option value={SCHEMA_UNAVAILABLE_VALUE}>{SCHEMA_UNAVAILABLE_LABEL}</option>
						{:else}
							<option value="">Choose column</option>
						{/if}
						{#each fallbackColumns as col}
							<option value={col}>{col}</option>
						{/each}
					{:else}
						{#each availableColumns as col}
							<option value={col}>{col}</option>
						{/each}
					{/if}
				</select>
				<button class="small" type="button" disabled={!selectedColumnToAdd} on:click={addSelectedColumn}>
					+
				</button>
			</div>
		</Field>

		{#each by as column, index}
			<div
				class="row"
				class:invalidRow={isMissingColumnError && missingByColumns.includes(column)}
				title={isMissingColumnError && missingByColumns.includes(column)
					? 'Column name not present in input schema'
					: undefined}
			>
				<Input
					value={column}
					className={isMissingColumnError && missingByColumns.includes(column) ? 'invalidInput' : ''}
					placeholder="column"
					onInput={(event) => {
						const next = [...by];
						next[index] = (event.currentTarget as HTMLInputElement).value;
						setDraft({ allColumns: false, by: next });
					}}
				/>
				<button
					class="small danger"
					type="button"
					on:click={() => {
						const next = by.filter((_, current) => current !== index);
						setDraft({ allColumns: false, by: next });
						setCommitAllowInvalid({ allColumns: false, by: next });
					}}
				>
					Remove
				</button>
				{#if isMissingColumnError && missingByColumns.includes(column) && availableColumns.length > 0}
					<select bind:value={replacementByIndex[index]}>
						{#each availableColumns as replacement}
							<option value={replacement}>{replacement}</option>
						{/each}
					</select>
					<button
						class="small"
						type="button"
						on:click={() => {
							const next = [...by];
							next[index] = replacementByIndex[index] ?? next[index];
							setDraft({ allColumns: false, by: next });
							setCommit({ allColumns: false, by: next });
						}}
					>
						Replace
					</button>
				{/if}
			</div>
		{/each}

		<div class="actions">
			<button
				class="small ghost"
				type="button"
				disabled={!canCommit}
				on:click={() => setCommit({ allColumns: false, by })}
			>
				Commit
			</button>
		</div>
		{#if !canCommit}
			<div class="warn">Select at least one column.</div>
		{/if}
		{#if by.includes(SCHEMA_UNAVAILABLE_VALUE)}
			<div class="warn">Select 1+ columns to dedupe on.</div>
		{/if}
		{#if isMissingColumnError && missingByColumns.length > 0}
			<div class="warn">Unknown columns: {missingByColumns.join(', ')}</div>
		{/if}
	{/if}
</Section>

<style>
	.row {
		display: flex;
		gap: 8px;
		align-items: center;
		margin: 8px 0;
	}

	.addRow {
		display: flex;
		gap: 8px;
		align-items: center;
		width: 100%;
		margin: 8px 0;
	}

	.addRow :global(select),
	.addRow :global(input) {
		flex: 1 1 auto;
		min-width: 0;
	}

	.toggle {
		display: inline-flex;
		gap: 8px;
		align-items: center;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	.actions {
		display: flex;
		gap: 8px;
		justify-content: space-between;
		align-items: center;
		margin-top: 8px;
		flex-wrap: wrap;
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

	button.ghost {
		background: transparent;
	}

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
	}

	button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.warn {
		font-size: 12px;
		color: #fca5a5;
		margin-top: 6px;
	}

	.invalidRow {
		border: 1px solid rgba(239, 68, 68, 0.6);
		border-radius: 8px;
		padding: 6px;
		background: rgba(239, 68, 68, 0.08);
	}

	.invalidInput {
		border-color: rgba(239, 68, 68, 0.7);
	}
</style>
