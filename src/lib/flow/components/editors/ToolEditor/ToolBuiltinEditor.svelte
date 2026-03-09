<script lang="ts">
	import type { ToolParams } from '$lib/flow/schema/tool';
	import {
		TOOL_BUILTIN_PROFILES,
		getBuiltinProfileById,
		type ToolBuiltinProfileId
	} from '$lib/flow/schema/toolBuiltinProfiles';
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

	$: builtin = params?.builtin ?? defaultBuiltin;
	$: profileId = (builtin.profileId ?? 'core') as ToolBuiltinProfileId;
	$: selectedProfile = getBuiltinProfileById(profileId);
	$: effectivePackages = profileId === 'custom' ? (builtin.customPackages ?? []) : selectedProfile.packages;
	$: effectivePackagesText = effectivePackages.join('\n');
	$: argsText = stringifyJson(builtin.args ?? {}, '{}');
	$: customPackagesHydrationSignature = JSON.stringify(builtin.customPackages ?? []);
	$: if (customPackagesHydrationSignature !== lastCustomPackagesHydrationSignature) {
		lastCustomPackagesHydrationSignature = customPackagesHydrationSignature;
		customPackagesDraft = (builtin.customPackages ?? []).join('\n');
		customPackagesErrors = [];
	}

	function commitArgs(text: string): void {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return;
		onCommit({ builtin: { ...builtin, args: parsed as Record<string, unknown> } });
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

	<Field label="included libs">
		<Input multiline={true} rows={6} value={effectivePackagesText} readonly={true} />
		<div class="hint">{effectivePackages.length} package(s)</div>
	</Field>

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

	<Field label="toolId">
		<Input
			value={builtin.toolId ?? ''}
			onInput={(event) => onDraft({ builtin: { ...builtin, toolId: (event.currentTarget as HTMLInputElement).value } })}
			onBlur={(event) => onCommit({ builtin: { ...builtin, toolId: (event.currentTarget as HTMLInputElement).value } })}
		/>
	</Field>

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
