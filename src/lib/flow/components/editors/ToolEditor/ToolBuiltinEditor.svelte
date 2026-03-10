<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import {
		TOOL_BUILTIN_PROFILES,
		getBuiltinProfileById,
		type ToolBuiltinProfileId
	} from '$lib/flow/schema/toolBuiltinProfiles';
	import {
		getBuiltinOperationById,
		getBuiltinOperationsForProfile
	} from '$lib/flow/schema/toolBuiltinCatalog';
	import { validateCustomPackageDraft } from '$lib/flow/schema/toolBuiltinCustomPackages';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { stringifyJson, tryParseJson } from '$lib/flow/components/editors/shared';

	type BuiltinParams = Extract<ToolParams, { provider: 'builtin' }>;

	export let params: Partial<BuiltinParams>;
	export let onDraft: (patch: Partial<BuiltinParams>) => void;
	export let onCommit: (patch: Partial<BuiltinParams>) => void;

	const defaultBuiltin: BuiltinParams['builtin'] = { toolId: '', args: {}, profileId: 'core', customPackages: [] };
	let customPackagesDraft = '';
	let customPackagesErrors: string[] = [];
	let advancedOpen = false;
	let lastCustomPackagesHydrationSignature = '';
	let operationArgsByToolId: Record<string, Record<string, unknown>> = {};

	$: builtin = params?.builtin ?? defaultBuiltin;
	$: profileId = (builtin.profileId ?? 'core') as ToolBuiltinProfileId;
	$: selectedProfile = getBuiltinProfileById(profileId);
	$: effectivePackages = profileId === 'custom' ? (builtin.customPackages ?? []) : selectedProfile.packages;
	$: availableOperations = getBuiltinOperationsForProfile(profileId);
	$: selectedOperation = getBuiltinOperationById(builtin.toolId);
	$: hasUnknownToolId = Boolean((builtin.toolId ?? '').trim()) && !selectedOperation;
	$: argsText = stringifyJson(builtin.args ?? {}, '{}');
	$: customPackagesHydrationSignature = JSON.stringify(builtin.customPackages ?? []);
	$: if (customPackagesHydrationSignature !== lastCustomPackagesHydrationSignature) {
		lastCustomPackagesHydrationSignature = customPackagesHydrationSignature;
		customPackagesDraft = (builtin.customPackages ?? []).join('\n');
		customPackagesErrors = [];
	}

	function rememberCurrentArgsForToolId(toolId: string, args: Record<string, unknown> | undefined): void {
		const key = (toolId ?? '').trim();
		if (!key) return;
		operationArgsByToolId = {
			...operationArgsByToolId,
			[key]: { ...(args ?? {}) }
		};
	}

	function commitArgs(text: string): void {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return;
		rememberCurrentArgsForToolId(builtin.toolId ?? '', parsed as Record<string, unknown>);
		onCommit({ builtin: { ...builtin, args: parsed as Record<string, unknown> } });
	}

	function cloneDefaultArgs(value: Record<string, unknown>): Record<string, unknown> {
		return JSON.parse(JSON.stringify(value ?? {})) as Record<string, unknown>;
	}

	function isEmptyArgs(argsValue: unknown): boolean {
		if (!argsValue || typeof argsValue !== 'object' || Array.isArray(argsValue)) return true;
		return Object.keys(argsValue as Record<string, unknown>).length === 0;
	}

	function handleProfileChange(event: Event): void {
		const nextProfileId = (event.currentTarget as HTMLSelectElement).value as ToolBuiltinProfileId;
		applyProfile(nextProfileId);
	}

	function applyProfile(nextProfileId: ToolBuiltinProfileId): void {
		const nextBuiltin: BuiltinParams['builtin'] = {
			...builtin,
			profileId: nextProfileId,
			customPackages: nextProfileId === 'custom' ? builtin.customPackages ?? [] : []
		};
		onDraft({ builtin: nextBuiltin });
		onCommit({ builtin: nextBuiltin });
	}

	function applyToolId(nextToolId: string): void {
		const prevToolId = (builtin.toolId ?? '').trim();
		if (prevToolId) {
			rememberCurrentArgsForToolId(prevToolId, builtin.args ?? {});
		}
		const operation = getBuiltinOperationById(nextToolId);
		const cachedArgs = nextToolId ? operationArgsByToolId[nextToolId] : undefined;
		const shouldSeedArgs = !cachedArgs && operation && isEmptyArgs(builtin.args);
		const nextBuiltin: BuiltinParams['builtin'] = {
			...builtin,
			toolId: nextToolId,
			args: cachedArgs ?? (shouldSeedArgs ? cloneDefaultArgs(operation.defaultArgs) : (builtin.args ?? {}))
		};
		onDraft({ builtin: nextBuiltin });
		onCommit({ builtin: nextBuiltin });
	}

	function handleToolChange(event: Event): void {
		const nextToolId = (event.currentTarget as HTMLSelectElement).value;
		applyToolId(nextToolId);
	}

	function seedExampleArgs(): void {
		const operation = getBuiltinOperationById(builtin.toolId);
		if (!operation) return;
		rememberCurrentArgsForToolId(builtin.toolId ?? '', operation.defaultArgs);
		const nextBuiltin: BuiltinParams['builtin'] = {
			...builtin,
			args: cloneDefaultArgs(operation.defaultArgs)
		};
		onDraft({ builtin: nextBuiltin });
		onCommit({ builtin: nextBuiltin });
	}

	function commitCustomPackages(text: string): void {
		const validation = validateCustomPackageDraft(text);
		customPackagesErrors = validation.errors;
		onCommit({
			builtin: {
				...builtin,
				profileId: 'custom',
				customPackages: validation.packages
			}
		});
	}
</script>

<Section title="Builtin">
	<div class="quickProfiles">
		<button type="button" class:active={profileId === 'core'} on:click={() => applyProfile('core')}>core</button>
		<button type="button" class:active={profileId === 'full'} on:click={() => applyProfile('full')}>llm</button>
		<button type="button" class:active={profileId === 'data'} on:click={() => applyProfile('data')}>dl</button>
		<button
			type="button"
			class:active={profileId === 'llm_finetune'}
			on:click={() => applyProfile('llm_finetune')}
		>
			finetune
		</button>
	</div>

	<Field label="profile">
		<select value={profileId} on:change={handleProfileChange}>
			{#each TOOL_BUILTIN_PROFILES as profile}
				<option value={profile.id}>{profile.label}</option>
			{/each}
		</select>
		<div class="hint">{selectedProfile.description}</div>
	</Field>

	<div class="includedLibsBlock" aria-label="included libs">
		<div class="packagePills" aria-label="included libraries">
			{#each effectivePackages as packageName}
				<span class="packagePill">{packageName}</span>
			{/each}
		</div>
		<div class="hint">{effectivePackages.length} package(s)</div>
	</div>

	{#if profileId === 'custom'}
		<details class="advancedBlock" bind:open={advancedOpen}>
			<summary>Advanced</summary>
			<Field label="custom libs">
				<Input
					multiline={true}
					rows={6}
					value={customPackagesDraft}
					placeholder="one package per line (or comma separated)"
					onInput={(event) => {
						customPackagesDraft = (event.currentTarget as HTMLTextAreaElement).value;
						customPackagesErrors = [];
					}}
					onBlur={(event) => {
						customPackagesDraft = (event.currentTarget as HTMLTextAreaElement).value;
						commitCustomPackages(customPackagesDraft);
					}}
				/>
				<div class="hint">Allowlisted package names only. Version specifiers are allowed.</div>
				{#if customPackagesErrors.length > 0}
					<div class="fieldError">
						{#each customPackagesErrors as err}
							<div>{err}</div>
						{/each}
					</div>
				{/if}
			</Field>
		</details>
	{/if}

	<div class="operationBlock">
		<select class="operationSelect" value={builtin.toolId ?? ''} on:change={handleToolChange}>
			<option value="">Select operation...</option>
			{#each availableOperations as operation}
				<option value={operation.id}>{operation.label} ({operation.id})</option>
			{/each}
		</select>
		{#if selectedOperation}
			<div class="hint">{selectedOperation.description}</div>
			<div class="opActions">
				<button type="button" on:click={seedExampleArgs}>Use Example Args</button>
			</div>
		{:else if hasUnknownToolId}
			<div class="hint">Custom operation id: {builtin.toolId}</div>
			<Input
				value={builtin.toolId ?? ''}
				onInput={(event) => onDraft({ builtin: { ...builtin, toolId: (event.currentTarget as HTMLInputElement).value } })}
				onBlur={(event) => onCommit({ builtin: { ...builtin, toolId: (event.currentTarget as HTMLInputElement).value } })}
			/>
		{/if}
	</div>

	<Field label="args">
		<Input multiline={true} rows={6} value={argsText} onBlur={(event) => commitArgs((event.currentTarget as HTMLTextAreaElement).value)} />
	</Field>
</Section>

<style>
	select {
		width: 100%;
		box-sizing: border-box;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(0, 0, 0, 0.2);
		color: inherit;
		padding: 8px 10px;
		font-size: 14px;
		min-height: 40px;
	}

	.hint {
		margin-top: 6px;
		font-size: 12px;
		opacity: 0.8;
	}

	.packagePills {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
	}

	.packagePill {
		display: inline-flex;
		align-items: center;
		border-radius: 999px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.06);
		padding: 4px 10px;
		font-size: 12px;
		line-height: 1.2;
	}

	.operationBlock {
		margin-bottom: 10px;
	}

	.operationSelect {
		width: 100%;
	}

	.includedLibsBlock {
		margin-bottom: 10px;
	}

	.quickProfiles {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
		margin-bottom: 10px;
	}

	.quickProfiles button {
		border: 1px solid rgba(255, 255, 255, 0.14);
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		border-radius: 8px;
		padding: 6px 8px;
		font-size: 12px;
		cursor: pointer;
		text-transform: lowercase;
	}

	.quickProfiles button.active {
		background: rgba(59, 130, 246, 0.2);
		border-color: rgba(59, 130, 246, 0.55);
	}

	.opActions {
		margin-top: 8px;
	}

	.opActions button {
		border: 1px solid rgba(255, 255, 255, 0.14);
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		border-radius: 8px;
		padding: 6px 8px;
		font-size: 12px;
		cursor: pointer;
	}

	.fieldError {
		margin-top: 6px;
		font-size: 12px;
		color: #f87171;
	}

	.advancedBlock {
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 10px;
		padding: 8px 10px;
		margin-bottom: 10px;
	}

	.advancedBlock summary {
		cursor: pointer;
		font-weight: 600;
	}
</style>
