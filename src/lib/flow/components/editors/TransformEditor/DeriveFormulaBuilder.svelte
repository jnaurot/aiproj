<script lang="ts">
	import {
		DERIVE_FORMULA_OPS,
		type DeriveArgSource,
		type DeriveFormulaOp,
		type DeriveRule,
		type DeriveRuleArg,
		validateRule
	} from './deriveRulesModel';

	export let rules: DeriveRule[] = [];
	export let columns: string[] = [];
	export let onChange: (next: DeriveRule[]) => void;

	function emit(next: DeriveRule[]): void {
		onChange(next);
	}

	function addRule(): void {
		const columnFallback = columns[0] ?? '';
		emit([
			...rules,
			{
				name: '',
				op: 'length',
				args: [{ source: 'column', column: columnFallback }]
			}
		]);
	}

	function removeRule(index: number): void {
		emit(rules.filter((_, current) => current !== index));
	}

	function updateRule(index: number, patch: Partial<DeriveRule>): void {
		const next = rules.map((rule, current) => (current === index ? { ...rule, ...patch } : rule));
		emit(next);
	}

	function addArg(index: number): void {
		const nextArgs = [...(rules[index]?.args ?? []), { source: 'literal', literalValue: '' } satisfies DeriveRuleArg];
		updateRule(index, { args: nextArgs });
	}

	function removeArg(ruleIndex: number, argIndex: number): void {
		const nextArgs = (rules[ruleIndex]?.args ?? []).filter((_, current) => current !== argIndex);
		updateRule(ruleIndex, { args: nextArgs });
	}

	function updateArg(ruleIndex: number, argIndex: number, patch: Partial<DeriveRuleArg>): void {
		const nextArgs = (rules[ruleIndex]?.args ?? []).map((arg, current) => {
			if (current !== argIndex) return arg;
			const next = { ...arg, ...patch };
			if (next.source === 'column') {
				delete next.literalValue;
				delete next.paramPath;
			}
			if (next.source === 'literal') {
				delete next.column;
				delete next.paramPath;
				next.literalValue = String(next.literalValue ?? '');
			}
			if (next.source === 'param_config') {
				delete next.column;
				delete next.literalValue;
				next.paramPath = String(next.paramPath ?? '');
			}
			return next;
		});
		updateRule(ruleIndex, { args: nextArgs });
	}
</script>

<div class="rulesWrap">
	{#if rules.length === 0}
		<div class="empty">No formula rules yet.</div>
	{/if}
	{#each rules as rule, index}
		{@const validationHints = validateRule(rule)}
		<div class="ruleCard">
			<div class="ruleTop">
				<input
					class="input"
					value={rule.name}
					placeholder="output column name"
					on:input={(event) => updateRule(index, { name: (event.currentTarget as HTMLInputElement).value })}
				/>
				<select
					class="select"
					value={rule.op}
					on:change={(event) => updateRule(index, { op: (event.currentTarget as HTMLSelectElement).value as DeriveFormulaOp })}
				>
					{#each DERIVE_FORMULA_OPS as spec}
						<option value={spec.value}>{spec.label}</option>
					{/each}
				</select>
				<button class="small danger" type="button" on:click={() => removeRule(index)}>Remove</button>
			</div>

			<div class="argsList">
				{#each rule.args as arg, argIndex}
					<div class="argRow">
						<select
							class="select"
							value={arg.source}
							on:change={(event) =>
								updateArg(index, argIndex, { source: (event.currentTarget as HTMLSelectElement).value as DeriveArgSource })}
						>
							<option value="literal">literal</option>
							<option value="column">column</option>
							<option value="param_config">param_config.path</option>
						</select>
						{#if arg.source === 'literal'}
							<input
								class="input"
								value={arg.literalValue ?? ''}
								placeholder="literal value"
								on:input={(event) => updateArg(index, argIndex, { literalValue: (event.currentTarget as HTMLInputElement).value })}
							/>
						{:else if arg.source === 'column'}
							<select
								class="select"
								value={arg.column ?? ''}
								on:change={(event) => updateArg(index, argIndex, { column: (event.currentTarget as HTMLSelectElement).value })}
							>
								<option value="" disabled selected={(arg.column ?? '').length === 0}>Select column</option>
								{#each columns as col}
									<option value={col}>{col}</option>
								{/each}
							</select>
						{:else}
							<input
								class="input"
								value={arg.paramPath ?? ''}
								placeholder="preferences.salary_min"
								on:input={(event) => updateArg(index, argIndex, { paramPath: (event.currentTarget as HTMLInputElement).value })}
							/>
						{/if}
						<button class="small ghost" type="button" on:click={() => removeArg(index, argIndex)}>x</button>
					</div>
				{/each}
			</div>

			<div class="ruleActions">
				<button class="small" type="button" on:click={() => addArg(index)}>+ Arg</button>
			</div>
			{#if validationHints.length > 0}
				<div class="warn">{validationHints.join(' ')}</div>
			{/if}
		</div>
	{/each}
	<div class="globalActions">
		<button class="small" type="button" on:click={addRule}>+ Add formula rule</button>
	</div>
</div>

<style>
	.rulesWrap {
		display: grid;
		gap: 8px;
		margin-top: 8px;
	}
	.empty {
		font-size: 12px;
		opacity: 0.75;
	}
	.ruleCard {
		border: 1px solid rgba(255, 255, 255, 0.14);
		border-radius: 10px;
		padding: 8px;
		display: grid;
		gap: 8px;
	}
	.ruleTop {
		display: grid;
		grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr) auto;
		gap: 6px;
	}
	.argsList {
		display: grid;
		gap: 6px;
	}
	.argRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) minmax(0, 1.4fr) auto;
		gap: 6px;
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
	.ruleActions,
	.globalActions {
		display: flex;
		justify-content: flex-end;
		gap: 8px;
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
	.warn {
		font-size: 12px;
		color: #fca5a5;
	}
</style>

