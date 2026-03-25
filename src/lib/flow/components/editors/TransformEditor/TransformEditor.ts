//lib/flow/components/editors/TransformEditor/TransformEditor.ts
import TransformFilterEditor from './TransformFilterEditor.svelte';
import TransformJsonFilterEditor from './TransformJsonFilterEditor.svelte';
import TransformSelectEditor from './TransformSelectEditor.svelte';
import TransformRenameEditor from './TransformRenameEditor.svelte';
import TransformDeriveEditor from './TransformDeriveEditor.svelte';
import TransformAggregateEditor from './TransformAggregateEditor.svelte';
import TransformJoinEditor from './TransformJoinEditor.svelte';
import TransformSortEditor from './TransformSortEditor.svelte';
import TransformLimitEditor from './TransformLimitEditor.svelte';
import TransformDedupeEditor from './TransformDedupeEditor.svelte';
import TransformNullPolicyEditor from './TransformNullPolicyEditor.svelte';
import TransformOutlierPolicyEditor from './TransformOutlierPolicyEditor.svelte';
import TransformTextCleanEditor from './TransformTextCleanEditor.svelte';
import TransformNlpNormalizeEditor from './TransformNlpNormalizeEditor.svelte';
import TransformTokenizeChunkEditor from './TransformTokenizeChunkEditor.svelte';
import TransformDatasetSplitEditor from './TransformDatasetSplitEditor.svelte';
import TransformClassImbalanceEditor from './TransformClassImbalanceEditor.svelte';
import TransformCategoricalEncodeEditor from './TransformCategoricalEncodeEditor.svelte';
import TransformNumericScaleEditor from './TransformNumericScaleEditor.svelte';
import TransformEmbeddingEditor from './TransformEmbeddingEditor.svelte';
import TransformFeatureSelectionEditor from './TransformFeatureSelectionEditor.svelte';
import TransformMlPreAdvancedEditor from './TransformMlPreAdvancedEditor.svelte';
import TransformSplitEditor from './TransformSplitEditor.svelte';
import TransformQualityGateEditor from './TransformQualityGateEditor.svelte';
import TransformMlContractEditor from './TransformMlContractEditor.svelte';
import TransformSqlEditor from './TransformSqlEditor.svelte';
import TransformJsonToTableEditor from './TransformJsonToTableEditor.svelte';
import TransformTextToTableEditor from './TransformTextToTableEditor.svelte';
import TransformTableToJsonEditor from './TransformTableToJsonEditor.svelte';
export {
	type EditorCommitMode,
	TransformEditorCommitModeByKind
} from '$lib/flow/editorCommitPolicy';

// filter, select, rename, derive, aggregate, join, sort, limit, dedupe, split, quality_gate, sql, json_to_table, text_to_table, table_to_json

export const TransformEditorByKind = {
    filter: TransformFilterEditor,
    json_filter: TransformJsonFilterEditor,
    select: TransformSelectEditor,
    rename: TransformRenameEditor,
    derive: TransformDeriveEditor,
    aggregate: TransformAggregateEditor,
    join: TransformJoinEditor,
    sort: TransformSortEditor,
    limit: TransformLimitEditor,
    dedupe: TransformDedupeEditor,
    null_policy: TransformNullPolicyEditor,
    outlier_policy: TransformOutlierPolicyEditor,
    text_clean: TransformTextCleanEditor,
    nlp_normalize: TransformNlpNormalizeEditor,
    tokenize_chunk: TransformTokenizeChunkEditor,
    dataset_split: TransformDatasetSplitEditor,
    class_imbalance: TransformClassImbalanceEditor,
    categorical_encode: TransformCategoricalEncodeEditor,
    numeric_scale: TransformNumericScaleEditor,
    embedding: TransformEmbeddingEditor,
    feature_selection: TransformFeatureSelectionEditor,
    leakage_detect: TransformMlPreAdvancedEditor,
    quality_profile: TransformMlPreAdvancedEditor,
    drift_compare: TransformMlPreAdvancedEditor,
    determinism_profile: TransformMlPreAdvancedEditor,
    fit_state_registry: TransformMlPreAdvancedEditor,
    pii_guard: TransformMlPreAdvancedEditor,
    inference_parity: TransformMlPreAdvancedEditor,
    split: TransformSplitEditor,
    quality_gate: TransformQualityGateEditor,
    ml_contract: TransformMlContractEditor,
    sql: TransformSqlEditor,
	json_to_table: TransformJsonToTableEditor,
	text_to_table: TransformTextToTableEditor,
	table_to_json: TransformTableToJsonEditor,
} as const;
