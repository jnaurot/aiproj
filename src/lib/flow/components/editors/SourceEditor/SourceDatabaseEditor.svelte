<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { SourceDatabaseParams, SourceOutputMode } from '$lib/flow/schema/source';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	type SourceDatabasePatch = Partial<SourceDatabaseParams>;

	export let selectedNode: Node<PipelineNodeData & Record<string, unknown>> | null;
	export let params: Partial<SourceDatabaseParams>;
	export let onDraft: (patch: SourceDatabasePatch) => void;
	export let onCommit: (patch: SourceDatabasePatch) => void;

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
	const outputModes: SourceOutputMode[] = ['table', 'text', 'json', 'binary'];
	const cursorTypes = ['auto', 'int', 'float', 'datetime', 'string'] as const;
	const partitionKinds = ['static_list', 'numeric_shards', 'date_range'] as const;
	const partitionErrorPolicies = ['fail_fast', 'skip_failed'] as const;

	function draft(patch: SourceDatabasePatch): void {
		onDraft?.(patch);
	}

	function commit(patch: SourceDatabasePatch): void {
		onCommit?.(patch);
	}
</script>

{#if selectedNode}
	<Section title="Database">
		<Field label="connection_string">
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
			<select
				value={String(incrementalEnabled)}
				on:change={(event) => {
					const enabled = (event.currentTarget as HTMLSelectElement).value === 'true';
					const patch = {
						incremental: {
							...(params?.incremental ?? {}),
							enabled
						}
					};
					draft(patch);
					commit(patch);
				}}
			>
				<option value="false">false</option>
				<option value="true">true</option>
			</select>
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
				<select
					value={incrementalCursorType}
					on:change={(event) => {
						const cursorType = (event.currentTarget as HTMLSelectElement).value;
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
				>
					{#each cursorTypes as ct}
						<option value={ct}>{ct}</option>
					{/each}
				</select>
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
			<select
				value={String(partitionEnabled)}
				on:change={(event) => {
					const enabled = (event.currentTarget as HTMLSelectElement).value === 'true';
					const patch = {
						partition: {
							...(params?.partition ?? {}),
							enabled
						}
					};
					draft(patch);
					commit(patch);
				}}
			>
				<option value="false">false</option>
				<option value="true">true</option>
			</select>
		</Field>

		{#if partitionEnabled}
			<Field label="partition.kind">
				<select
					value={partitionKind}
					on:change={(event) => {
						const kind = (event.currentTarget as HTMLSelectElement).value;
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
				>
					{#each partitionKinds as pk}
						<option value={pk}>{pk}</option>
					{/each}
				</select>
			</Field>

			<Field label="partition.on_error">
				<select
					value={partitionOnError}
					on:change={(event) => {
						const on_error = (event.currentTarget as HTMLSelectElement).value;
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
				>
					{#each partitionErrorPolicies as policy}
						<option value={policy}>{policy}</option>
					{/each}
				</select>
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
			<select
				value={outputMode}
				on:change={(event) => {
					const mode = (event.currentTarget as HTMLSelectElement).value as SourceOutputMode;
					draft({ output: { ...(params?.output ?? {}), mode } });
					commit({ output: { ...(params?.output ?? {}), mode } });
				}}
			>
				{#each outputModes as mode}
					<option value={mode}>{mode}</option>
				{/each}
			</select>
		</Field>

		<p class="hint">
			Backend requires: (connection_string OR connection_ref) AND (query OR table_name).
		</p>
	</Section>
{/if}

<style>
	.hint {
		margin-top: 8px;
		font-size: 12px;
		opacity: 0.75;
	}
</style>
