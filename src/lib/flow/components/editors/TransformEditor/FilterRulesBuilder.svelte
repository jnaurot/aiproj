<script lang="ts">
	import type { FilterCondition, FilterGroup, FilterOperator, FilterRuleNode, FilterValueSource } from './filterRulesModel';
	import { FILTER_OPERATORS } from './filterRulesModel';

	export let group: FilterGroup;
	export let columns: Array<{ name: string; type: string }> = [];
	export let depth = 0;
	export let onChange: (next: FilterGroup) => void;
	export let onRemoveGroup: (() => void) | null = null;

	const VALUE_OPERATORS = new Set(FILTER_OPERATORS.filter((entry) => entry.needsValue).map((entry) => entry.value));
	const groupOpSelectId = `filter-group-op-${Math.random().toString(36).slice(2, 10)}`;

	function emitGroup(next: FilterGroup): void {
		onChange(next);
	}

	function updateGroupOp(op: 'all' | 'any'): void {
		emitGroup({ ...group, op });
	}

	function addCondition(): void {
		const nextCondition: FilterCondition = {
			kind: 'condition',
			column: columns[0]?.name ?? '',
			op: 'eq',
			valueSource: 'literal',
			literalValue: ''
		};
		emitGroup({ ...group, conditions: [...group.conditions, nextCondition] });
	}

	function addGroup(): void {
		const nextGroup: FilterGroup = {
			kind: 'group',
			op: 'all',
			conditions: []
		};
		emitGroup({ ...group, conditions: [...group.conditions, nextGroup] });
	}

	function removeChild(index: number): void {
		const next = group.conditions.filter((_, i) => i !== index);
		emitGroup({ ...group, conditions: next });
	}

	function updateChild(index: number, node: FilterRuleNode): void {
		const next = group.conditions.map((item, i) => (i === index ? node : item));
		emitGroup({ ...group, conditions: next });
	}

	function needsValue(op: FilterOperator): boolean {
		return VALUE_OPERATORS.has(op);
	}

	function updateCondition(node: FilterCondition, patch: Partial<FilterCondition>): FilterCondition {
		const next = { ...node, ...patch };
		if (!needsValue(next.op)) {
			delete next.literalValue;
			delete next.paramPath;
		} else {
			next.valueSource = next.valueSource === 'param_config' ? 'param_config' : 'literal';
			next.literalValue = String(next.literalValue ?? '');
			next.paramPath = String(next.paramPath ?? '');
		}
		return next;
	}

	function opLabel(value: FilterOperator): string {
		const match = FILTER_OPERATORS.find((entry) => entry.value === value);
		return match?.label ?? value;
	}

	function getValuePlaceholder(op: FilterOperator): string {
		if (op === 'in' || op === 'not_in') return 'comma values: full-time,contract';
		if (op === 'regex') return 'pattern (duckdb regex)';
		if (op === 'contains') return 'substring';
		return 'value';
	}
</script>

<div class="group" style={`--depth:${depth};`}>
	<div class="groupHeader">
		<label class="tiny" for={groupOpSelectId}>Match</label>
		<select
			id={groupOpSelectId}
			class="select"
			value={group.op}
			on:change={(event) => updateGroupOp((event.currentTarget as HTMLSelectElement).value === 'any' ? 'any' : 'all')}
		>
			<option value="all">all conditions</option>
			<option value="any">any condition</option>
		</select>
		{#if onRemoveGroup}
			<button class="small danger" type="button" on:click={() => onRemoveGroup?.()}>Remove group</button>
		{/if}
	</div>

	<div class="groupBody">
		{#if group.conditions.length === 0}
			<div class="empty">No conditions yet.</div>
		{:else}
			{#each group.conditions as node, index}
				{#if node.kind === 'condition'}
					<div class="conditionRow">
						<select
							class="select"
							value={node.column}
							on:change={(event) =>
								updateChild(
									index,
									updateCondition(node, { column: (event.currentTarget as HTMLSelectElement).value })
								)}
						>
							<option value="" disabled selected={node.column.length === 0}>Select column</option>
							{#each columns as col}
								<option value={col.name}>{col.name}</option>
							{/each}
						</select>
						<select
							class="select"
							value={node.op}
							on:change={(event) =>
								updateChild(
									index,
									updateCondition(node, {
										op: (event.currentTarget as HTMLSelectElement).value as FilterOperator
									})
								)}
						>
							{#each FILTER_OPERATORS as op}
								<option value={op.value}>{op.label}</option>
							{/each}
						</select>
						{#if needsValue(node.op)}
							<select
								class="select"
								value={node.valueSource ?? 'literal'}
								on:change={(event) =>
									updateChild(
										index,
										updateCondition(node, {
											valueSource:
												((event.currentTarget as HTMLSelectElement).value as FilterValueSource) === 'param_config'
													? 'param_config'
													: 'literal'
										})
									)}
							>
								<option value="literal">literal</option>
								<option value="param_config">param_config.path</option>
							</select>
							{#if (node.valueSource ?? 'literal') === 'literal'}
								<input
									class="input"
									value={node.literalValue ?? ''}
									placeholder={getValuePlaceholder(node.op)}
									on:input={(event) =>
										updateChild(
											index,
											updateCondition(node, { literalValue: (event.currentTarget as HTMLInputElement).value })
										)}
								/>
							{:else}
								<input
									class="input"
									value={node.paramPath ?? ''}
									placeholder="preferences.salary_min"
									on:input={(event) =>
										updateChild(
											index,
											updateCondition(node, { paramPath: (event.currentTarget as HTMLInputElement).value })
										)}
								/>
							{/if}
						{:else}
							<div class="noValue">{opLabel(node.op)}</div>
						{/if}
						<button class="small danger" type="button" on:click={() => removeChild(index)}>Remove</button>
					</div>
				{:else}
					<div class="nested">
						<svelte:self
							group={node}
							columns={columns}
							depth={depth + 1}
							onChange={(next) => updateChild(index, next)}
							onRemoveGroup={() => removeChild(index)}
						/>
					</div>
				{/if}
			{/each}
		{/if}
	</div>

	<div class="groupActions">
		<button class="small" type="button" on:click={addCondition}>+ Condition</button>
		<button class="small ghost" type="button" on:click={addGroup}>+ Group</button>
	</div>
</div>

<style>
	.group {
		border: 1px solid rgba(255, 255, 255, 0.14);
		border-radius: 10px;
		padding: 10px;
		margin-top: 8px;
		background: rgba(255, 255, 255, 0.02);
	}
	.groupHeader {
		display: flex;
		gap: 8px;
		align-items: center;
		flex-wrap: wrap;
	}
	.groupBody {
		margin-top: 8px;
		display: grid;
		gap: 8px;
	}
	.conditionRow {
		display: grid;
		grid-template-columns: repeat(4, minmax(0, 1fr)) auto;
		gap: 6px;
		align-items: center;
	}
	.select,
	.input {
		width: 100%;
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		border: 1px solid rgba(255, 255, 255, 0.16);
		border-radius: 8px;
		padding: 6px 8px;
		font-size: 12px;
	}
	.tiny,
	.empty,
	.noValue {
		font-size: 12px;
		opacity: 0.8;
	}
	.nested {
		margin-left: 8px;
	}
	.groupActions {
		display: flex;
		gap: 8px;
		margin-top: 8px;
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
</style>
