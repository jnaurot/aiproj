<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceDatabaseParams, SourceOutputMode } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ThemedSelect, { type ThemedSelectOption } from '$lib/flow/components/ui/ThemedSelect.svelte';
	import SourceCapabilityBanner from './SourceCapabilityBanner.svelte';
	import SourceEffectivePreview from './SourceEffectivePreview.svelte';
	import { effectiveConfigForSource } from './sourceEffectiveConfig';
	import {
		sourceControlFromParamPath,
		sourceDatabaseValidationHints
	} from './sourceValidationHints';
	import { emitSourceEditorTelemetry, makeValidationEvent } from './sourceEditorTelemetry';
	import { asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	type SourceDatabasePatch = Partial<SourceDatabaseParams>;

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let params: Partial<SourceDatabaseParams>;
	export let onDraft: (patch: SourceDatabasePatch) => void;
	export let onCommit: (patch: SourceDatabasePatch) => void;
	export let nodeError: Record<string, unknown> | null = null;

	$: void onCommit;
	$: connection_string = asString(params?.connection_string, '');
	$: connection_ref = asString(params?.connection_ref, '');
	$: query = asString(params?.query, '');
	$: table_name = asString(params?.table_name, '');
	$: limit = asNumberOrEmpty(params?.limit);
	$: incrementalEnabled = Boolean(params?.incremental?.enabled ?? false);
	$: incrementalStateKey = asString(params?.incremental?.state_key, '');
	$: incrementalCursorColumn = asString(params?.incremental?.cursor_column, '');
	$: incrementalCursorType = asString(params?.incremental?.cursor_type, 'auto');
	$: partitionEnabled = Boolean(params?.partition?.enabled ?? false);
	$: partitionKind = asString(params?.partition?.kind, 'static_list');
	$: partitionOnError = asString(params?.partition?.on_error, 'fail_fast');
	$: partitionValues = Array.isArray(params?.partition?.static_values) ? (params?.partition?.static_values ?? []).join(',') : '';
	$: partitionBindKey = asString(params?.partition?.bind_key, 'partition');
	$: partitionParallelism = asNumberOrEmpty(params?.partition?.parallelism_cap ?? 2);
	$: outputMode = (asString(params?.output?.mode, 'table') as SourceOutputMode) ?? 'table';
	$: effectiveConfigLines = effectiveConfigForSource('database', params as Record<string, unknown>);
	$: validationHints = sourceDatabaseValidationHints(params);
	$: highlightedControl = sourceControlFromParamPath(
		'database',
		asString((nodeError as any)?.paramPath, '')
	);
	$: activeNodeId = asString(selectedNode?.id, '');
	let previousValidationHintKeys = new Set<string>();
	$: {
		const nextKeys = new Set<string>();
		for (const hint of validationHints) {
			const key = `${hint.controlId}::${hint.level}::${hint.message}`;
			nextKeys.add(key);
			if (activeNodeId && !previousValidationHintKeys.has(key)) {
				emitSourceEditorTelemetry(
					makeValidationEvent(
						'database',
						activeNodeId,
						hint.controlId,
						hint.level as 'info' | 'warning' | 'error',
						'shown'
					)
				);
			}
		}
		if (activeNodeId) {
			for (const key of previousValidationHintKeys) {
				if (nextKeys.has(key)) continue;
				const [controlId, level] = key.split('::');
				emitSourceEditorTelemetry(
					makeValidationEvent(
						'database',
						activeNodeId,
						controlId ?? 'unknown',
						((level as 'info' | 'warning' | 'error') ?? 'warning'),
						'resolved'
					)
				);
			}
		}
		previousValidationHintKeys = nextKeys;
	}
	const outputModes: SourceOutputMode[] = ['table', 'text', 'json', 'binary'];
	const cursorTypes = ['auto', 'int', 'float', 'datetime', 'string'] as const;
	const partitionKinds = ['static_list', 'numeric_shards', 'date_range'] as const;
	const partitionErrorPolicies = ['fail_fast', 'skip_failed'] as const;
	const booleanOptions: ThemedSelectOption[] = [
		{ value: 'false', label: 'false' },
		{ value: 'true', label: 'true' }
	];
	const cursorTypeOptions: ThemedSelectOption[] = cursorTypes.map((value) => ({ value, label: value }));
	const partitionKindOptions: ThemedSelectOption[] = partitionKinds.map((value) => ({ value, label: value }));
	const partitionPolicyOptions: ThemedSelectOption[] = partitionErrorPolicies.map((value) => ({ value, label: value }));
	const outputModeOptions: ThemedSelectOption[] = outputModes.map((value) => ({ value, label: value }));

	function draft(patch: SourceDatabasePatch): void {
		onDraft?.(patch);
	}

	function commit(patch: SourceDatabasePatch): void {
		onCommit?.(patch);
	}
</script>

{#if selectedNode}
	<Section title="Database">
		<SourceCapabilityBanner sourceKind="database" params={params as Record<string, unknown>} />
		<Field label="connection_string">
			{#if highlightedControl === 'connection'}
				<div class="warn">Backend validation flagged connection settings for this node.</div>
			{/if}
			<Input
				value={connection_string}
				placeholder="postgresql://user:pass@host:5432/db"
				onInput={(event) =>
					draft({ connection_string: (event.currentTarget as HTMLInputElement).value })}

			/>
		</Field>

		<Field label="connection_ref">
			<Input
				value={connection_ref}
				placeholder="(optional) secret/env ref"
				onInput={(event) => {
					const value = (event.currentTarget as HTMLInputElement).value.trim();
					draft({ connection_ref: value === '' ? undefined : value });
				}}

			/>
		</Field>

		<Field label="query">
			{#if highlightedControl === 'input'}
				<div class="warn">Backend validation flagged input selection (query/table_name).</div>
			{/if}
			<Input
				multiline={true}
				rows={4}
				value={query}
				placeholder="SELECT * FROM table"
				onInput={(event) => {
					const value = (event.currentTarget as HTMLTextAreaElement).value;
					const nextQuery = value.trim();
					const patch = {
						query: nextQuery === '' ? undefined : value,
						table_name: nextQuery ? undefined : params?.table_name
					};
					draft(patch);
					commit(patch);
				}}

			/>
		</Field>

		<Field label="table_name">
			<Input
				value={table_name}
				placeholder="(optional) table"
				onInput={(event) => {
					const value = (event.currentTarget as HTMLInputElement).value;
					const nextTable = value.trim();
					const patch = {
						table_name: nextTable === '' ? undefined : value,
						query: nextTable ? undefined : params?.query
					};
					draft(patch);
					commit(patch);
				}}

			/>
		</Field>

		<Field label="limit">
			<Input
				type="number"
				min="1"
				step="1"
				value={limit}
				placeholder="e.g. 5000"
				onInput={(event) =>
					draft({ limit: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}

			/>
		</Field>

		<Field label="incremental.enabled">
			<ThemedSelect
				value={String(incrementalEnabled)}
				options={booleanOptions}
				ariaLabel="incremental enabled"
				onValueChange={(next) => {
					const enabled = String(next) === 'true';
					const patch = {
						incremental: {
							...(params?.incremental ?? {}),
							enabled
						}
					};
					draft(patch);
					commit(patch);
				}}
			/>
		</Field>

		{#if incrementalEnabled}
			<Field label="incremental.cursor_column">
				<Input
					value={incrementalCursorColumn}
					placeholder="updated_at or id"
					onInput={(event) =>
						draft({
							incremental: {
								...(params?.incremental ?? {}),
								enabled: true,
								cursor_column: (event.currentTarget as HTMLInputElement).value.trim()
							}
						})}

				/>
			</Field>

			<Field label="incremental.cursor_type">
				<ThemedSelect
					value={incrementalCursorType}
					options={cursorTypeOptions}
					ariaLabel="incremental cursor type"
					onValueChange={(cursorType) => {
						const patch = {
							incremental: {
								...(params?.incremental ?? {}),
								enabled: true,
								cursor_type: cursorType
							}
						};
						draft(patch);
						commit(patch);
					}}
				/>
			</Field>

			<Field label="incremental.state_key">
				<Input
					value={incrementalStateKey}
					placeholder="optional state key override"
					onInput={(event) =>
						draft({
							incremental: {
								...(params?.incremental ?? {}),
								enabled: true,
								state_key: (event.currentTarget as HTMLInputElement).value.trim() || undefined
							}
						})}

				/>
			</Field>
		{/if}

		<Field label="partition.enabled">
			<ThemedSelect
				value={String(partitionEnabled)}
				options={booleanOptions}
				ariaLabel="partition enabled"
				onValueChange={(next) => {
					const enabled = String(next) === 'true';
					const patch = {
						partition: {
							...(params?.partition ?? {}),
							enabled
						}
					};
					draft(patch);
					commit(patch);
				}}
			/>
		</Field>

		{#if partitionEnabled}
			<Field label="partition.kind">
				<ThemedSelect
					value={partitionKind}
					options={partitionKindOptions}
					ariaLabel="partition kind"
					onValueChange={(kind) => {
						const patch = {
							partition: {
								...(params?.partition ?? {}),
								enabled: true,
								kind
							}
						};
						draft(patch);
						commit(patch);
					}}
				/>
			</Field>

			<Field label="partition.on_error">
				<ThemedSelect
					value={partitionOnError}
					options={partitionPolicyOptions}
					ariaLabel="partition error policy"
					onValueChange={(on_error) => {
						const patch = {
							partition: {
								...(params?.partition ?? {}),
								enabled: true,
								on_error
							}
						};
						draft(patch);
						commit(patch);
					}}
				/>
			</Field>

			<Field label="partition.bind_key">
				<Input
					value={partitionBindKey}
					placeholder="partition"
					onInput={(event) =>
						draft({
							partition: {
								...(params?.partition ?? {}),
								enabled: true,
								bind_key: (event.currentTarget as HTMLInputElement).value.trim() || 'partition'
							}
						})}

				/>
			</Field>

			<Field label="partition.parallelism_cap">
				<Input
					type="number"
					min="1"
					step="1"
					value={partitionParallelism}
					onInput={(event) =>
						draft({
							partition: {
								...(params?.partition ?? {}),
								enabled: true,
								parallelism_cap: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) ?? 2
							}
						})}

				/>
			</Field>

			{#if partitionKind === 'static_list'}
				<Field label="partition.static_values">
					<Input
						value={partitionValues}
						placeholder="a,b,c"
						onInput={(event) =>
							draft({
								partition: {
									...(params?.partition ?? {}),
									enabled: true,
									kind: 'static_list',
									static_values: String((event.currentTarget as HTMLInputElement).value ?? '')
										.split(',')
										.map((s) => s.trim())
										.filter((s) => s.length > 0)
								}
							})}

					/>
				</Field>
			{/if}
		{/if}

		<Field label="output mode">
			<ThemedSelect
				value={outputMode}
				options={outputModeOptions}
				ariaLabel="database output mode"
				onValueChange={(next) => {
					const mode = String(next) as SourceOutputMode;
					draft({ output: { ...(params?.output ?? {}), mode } });
					commit({ output: { ...(params?.output ?? {}), mode } });
				}}
			/>
		</Field>

		<p class="hint">
			Backend requires: (connection_string OR connection_ref) AND (query OR table_name).
		</p>
		{#if validationHints.length > 0}
			<div class="hintList">
				{#each validationHints as hint}
					<div class={hint.level === 'error' ? 'warn' : 'hint'}>
						[{hint.controlId}] {hint.message}
					</div>
				{/each}
			</div>
		{/if}
		<SourceEffectivePreview lines={effectiveConfigLines} />
	</Section>
{/if}

<style>
	.hint {
		margin-top: 8px;
		font-size: 12px;
		opacity: 0.75;
	}

	.hintList {
		display: grid;
		gap: 4px;
		margin-top: 6px;
	}

	.warn {
		font-size: 12px;
		color: var(--color-danger, #f87171);
	}
</style>
