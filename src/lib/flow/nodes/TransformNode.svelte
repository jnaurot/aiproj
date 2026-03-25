<script lang="ts">
	import type { TransformNodeData } from '$lib/flow/types';
	import BaseNode from './BaseNode.svelte';

	export let data: TransformNodeData;
	export let id: string;
	export let selected: boolean = false;

	function countFilterConditions(node: unknown): number {
		if (!node || typeof node !== 'object') return 0;
		const record = node as Record<string, unknown>;
		if (record.kind === 'condition') return 1;
		if (record.kind !== 'group') return 0;
		const children = Array.isArray(record.conditions) ? record.conditions : [];
		return children.reduce((sum, child) => sum + countFilterConditions(child), 0);
	}

	function summary(op: TransformNodeData['params']['op'], params: Record<string, any>): string {
		if (op === 'filter') {
			const mode = String(params.filter?.mode ?? '').trim();
			if (mode === 'rules') {
				const count = countFilterConditions(params.filter?.rules);
				return count > 0 ? `Rules (${count} condition${count === 1 ? '' : 's'})` : 'Rules';
			}
			return params.filter?.expr ?? '-';
		}
		if (op === 'json_filter') {
			const count = countFilterConditions(params.json_filter?.rules);
			return count > 0 ? `JSON Rules (${count} condition${count === 1 ? '' : 's'})` : 'JSON Rules';
		}
		if (op === 'select') return (params.select?.columns || []).join(', ') || '-';
		if (op === 'rename') return Object.keys(params.rename?.map || {}).length > 0 ? 'Rename columns' : '-';
		if (op === 'derive') return (params.derive?.columns || []).length > 0 ? 'Derive columns' : '-';
		if (op === 'aggregate') return (params.aggregate?.metrics || []).length > 0 ? 'Aggregate' : '-';
		if (op === 'join') return (params.join?.clauses || []).length > 0 ? 'Join' : '-';
		if (op === 'sort') return (params.sort?.by || []).length > 0 ? 'Sort' : '-';
		if (op === 'limit') return params.limit?.n ? `Limit ${params.limit.n}` : '-';
		if (op === 'dedupe') return 'Deduplicate';
		if (op === 'null_policy') return `Null policy: ${String(params.null_policy?.mode ?? 'report')}`;
		if (op === 'outlier_policy') return `Outlier policy: ${String(params.outlier_policy?.mode ?? 'clip')}`;
		if (op === 'text_clean') return 'Text clean';
		if (op === 'nlp_normalize') return `NLP normalize (${String(params.nlp_normalize?.language ?? 'en')})`;
		if (op === 'tokenize_chunk') return `Tokenize chunk (${Number(params.tokenize_chunk?.maxTokens ?? 256)} max)`;
		if (op === 'dataset_split') return `Split (${String(params.dataset_split?.strategy ?? 'random')})`;
		if (op === 'class_imbalance') return `Class imbalance (${String(params.class_imbalance?.strategy ?? 'report')})`;
		if (op === 'categorical_encode') return `Categorical encode (${String(params.categorical_encode?.encoding ?? 'one_hot')})`;
		if (op === 'numeric_scale') return `Numeric scale (${String(params.numeric_scale?.method ?? 'standard')})`;
		if (op === 'embedding') return `Embedding (${Number(params.embedding?.dimensions ?? 16)}d)`;
		if (op === 'feature_selection') return `Feature select (${String(params.feature_selection?.method ?? 'variance')})`;
		if (op === 'leakage_detect') return 'Leakage detect';
		if (op === 'quality_profile') return 'Quality profile';
		if (op === 'drift_compare') return 'Drift compare';
		if (op === 'determinism_profile') return 'Determinism profile';
		if (op === 'fit_state_registry') return `Fit state (${String(params.fit_state_registry?.mode ?? 'fit')})`;
		if (op === 'pii_guard') return `PII guard (${String(params.pii_guard?.action ?? 'report')})`;
		if (op === 'inference_parity') return 'Inference parity';
		if (op === 'split') return 'Split text';
		if (op === 'quality_gate') return (params.quality_gate?.checks || []).length > 0 ? 'Quality gate' : '-';
		if (op === 'ml_contract') {
			const label = String(params.ml_contract?.labelColumn ?? 'label').trim() || 'label';
			const featureCount = Array.isArray(params.ml_contract?.featureColumns)
				? params.ml_contract.featureColumns.length
				: 0;
			return `ML contract (${label}, ${featureCount} features)`;
		}
		if (op === 'sql') return params.sql?.query ? 'SQL Query' : '-';
		if (op === 'json_to_table') return 'JSON to table';
		if (op === 'text_to_table') return 'Text to table';
		if (op === 'table_to_json') return 'Table to JSON';
		return '-';
	}

	$: label = summary(data?.params?.op ?? 'filter', data?.params ?? {});
</script>

<BaseNode {id} {data} {selected}>
	<div style="font-size:12px; opacity:0.85;">
		{label}
	</div>
</BaseNode>
