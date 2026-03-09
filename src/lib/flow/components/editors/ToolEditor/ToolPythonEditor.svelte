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

	type PythonParams = Extract<ToolParams, { provider: 'python' }>;

	export let params: Partial<PythonParams>;
	export let onDraft: (patch: Partial<PythonParams>) => void;
	export let onCommit: (patch: Partial<PythonParams>) => void;

	const defaultPython: PythonParams['python'] = {
		code: '',
		args: {},
		capture_output: true
	};

	let argsDraft = '{}';
	let argsError: string | null = null;
	let customPackagesDraft = '';
	let customPackagesErrors: string[] = [];
	let lastCustomPackagesHydrationSignature = '';
	let lastArgsHydrationSignature = '';

	$: python = params?.python ?? defaultPython;
	$: builtin = (params?.builtin ?? { profileId: 'core', customPackages: [] }) as NonNullable<PythonParams['builtin']>;
	$: profileId = (builtin.profileId ?? 'core') as ToolBuiltinProfileId;
	$: selectedProfile = getBuiltinProfileById(profileId);
	$: concisePackages = (selectedProfile.packages ?? []).slice(0, 4).join(', ');
	$: captureOutput = Boolean(python.capture_output ?? true);
	$: customPackagesHydrationSignature = JSON.stringify(builtin.customPackages ?? []);
	$: if (customPackagesHydrationSignature !== lastCustomPackagesHydrationSignature) {
		lastCustomPackagesHydrationSignature = customPackagesHydrationSignature;
		customPackagesDraft = (builtin.customPackages ?? []).join('\n');
		customPackagesErrors = [];
	}
	$: argsHydrationSignature = JSON.stringify(python.args ?? {});
	$: if (argsHydrationSignature !== lastArgsHydrationSignature) {
		lastArgsHydrationSignature = argsHydrationSignature;
		argsDraft = stringifyJson(python.args ?? {}, '{}');
		argsError = null;
	}

	function validateArgsJson(text: string): { value?: Record<string, unknown>; error?: string } {
		const parsed = tryParseJson(text);
		if (parsed === undefined) return { error: 'invalid JSON' };
		if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
			return { error: 'args must be a JSON object' };
		}
		return { value: parsed as Record<string, unknown> };
	}

	function handleProfileChange(event: Event): void {
		const nextProfileId = (event.currentTarget as HTMLSelectElement).value as ToolBuiltinProfileId;
		applyProfile(nextProfileId);
	}

	function applyProfile(nextProfileId: ToolBuiltinProfileId): void {
		const nextBuiltin = {
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

<Section title="Python">
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

	<Field label="Builtin Profile">
		<select value={profileId} on:change={handleProfileChange}>
			{#each TOOL_BUILTIN_PROFILES as profile}
				<option value={profile.id}>{profile.label}</option>
			{/each}
		</select>
		<div class="hint">
			{selectedProfile.description}
			{#if concisePackages}
				<span> Included: {concisePackages}{selectedProfile.packages.length > 4 ? ', ...' : ''}</span>
			{/if}
		</div>
	</Field>

	{#if profileId === 'custom'}
		<details class="advancedBlock">
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

	<Field label="code">
		<Input
			multiline={true}
			rows={10}
			value={python.code ?? ''}
			onInput={(event) => onDraft({ python: { ...python, code: (event.currentTarget as HTMLTextAreaElement).value } })}
			onBlur={(event) => onCommit({ python: { ...python, code: (event.currentTarget as HTMLTextAreaElement).value } })}
		/>
	</Field>

	<Field label="capture_output">
		<Input
			type="checkbox"
			checked={captureOutput}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ python: { ...python, capture_output: checked } });
				onCommit({ python: { ...python, capture_output: checked } });
			}}
		/>
	</Field>

	<Field label="args">
		<Input
			multiline={true}
			rows={6}
			value={argsDraft}
			onInput={(event) => {
				argsDraft = (event.currentTarget as HTMLTextAreaElement).value;
				argsError = validateArgsJson(argsDraft).error ?? null;
			}}
			onBlur={(event) => {
				argsDraft = (event.currentTarget as HTMLTextAreaElement).value;
				const validated = validateArgsJson(argsDraft);
				argsError = validated.error ?? null;
				if (!argsError && validated.value) onCommit({ python: { ...python, args: validated.value } });
			}}
		/>
		{#if argsError}
			<div class="fieldError">{argsError}</div>
		{/if}
	</Field>
</Section>

<style>
	.fieldError {
		margin-top: 6px;
		font-size: 12px;
		color: #f87171;
	}

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
