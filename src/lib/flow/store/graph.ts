import type { Node } from "@xyflow/svelte";
import type { PipelineNodeData } from "$lib/flow/types";
import { normalizeWithDefaults } from "$lib/flow/util/normalize";
import { defaultSourceParamsByKind } from "$lib/flow/schema/sourceDefaults";
import { SourceParamsSchemaByKind } from "$lib/flow/schema/source";
import { defaultLlmParamsByKind, defaultModelParamsByKind } from "$lib/flow/schema/llmDefaults";
import { LlmParamsSchemaByKind } from "$lib/flow/schema/llm";
import { defaultTransformParamsByKind } from "$lib/flow/schema/transformDefaults"
import { TransformParamsSchema } from "$lib/flow/schema/transform"
import { ToolParamsSchema } from "$lib/flow/schema/tool";
import { defaultToolParamsByProvider } from "$lib/flow/schema/toolDefaults";
import { ComponentParamsSchema } from "$lib/flow/schema/component";
import { defaultComponentParams } from "$lib/flow/schema/componentDefaults";

// pick schema + defaults by kind
type Pick = {
  schema: unknown;
  defaults: unknown;
};

export function pickValidation(
  data: PipelineNodeData,
  patch: unknown,
  existing: unknown
): Pick {
  switch (data.kind) {
    case "source": {
      const sk = data.sourceKind ?? "file";
      return {
        schema: SourceParamsSchemaByKind[sk],
        defaults: defaultSourceParamsByKind[sk]
      };
    }

    case "transform": {
      const tk = data.transformKind ?? "filter"
      return { 
        schema: TransformParamsSchema,
        defaults: defaultTransformParamsByKind[tk] };
    }

    case "llm":
    case "model": {
      const lk = data.llmKind ?? "ollama"
      const mk = (data as any).modelKind ?? "llm"
      return {
        schema: LlmParamsSchemaByKind[lk],
        defaults: data.kind === "model" ? defaultModelParamsByKind[mk] : defaultLlmParamsByKind[lk]
      };
    }

    case "tool": {
      const patchProvider =
        typeof (patch as any)?.provider === "string" ? (patch as any).provider : undefined;
      const existingProvider =
        typeof (existing as any)?.provider === "string" ? (existing as any).provider : undefined;
      const provider = (patchProvider ?? existingProvider ?? "mcp") as keyof typeof defaultToolParamsByProvider;
      const defaults = defaultToolParamsByProvider[provider] ?? defaultToolParamsByProvider.mcp;
      return { schema: ToolParamsSchema, defaults };
    }

    case "component":
      return { schema: ComponentParamsSchema, defaults: defaultComponentParams };

    default:
      return { schema: ToolParamsSchema, defaults: defaultToolParamsByProvider.mcp };
  }
}

function isObject(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === "object" && !Array.isArray(v);
}

const OP_TO_BLOCK: Record<string, string> = {
  filter: "filter",
  select: "select",
  rename: "rename",
  derive: "derive",
  aggregate: "aggregate",
  join: "join",
  sort: "sort",
  limit: "limit",
  dedupe: "dedupe",
  null_policy: "null_policy",
  outlier_policy: "outlier_policy",
  text_clean: "text_clean",
  nlp_normalize: "nlp_normalize",
  tokenize_chunk: "tokenize_chunk",
  dataset_split: "dataset_split",
  class_imbalance: "class_imbalance",
  categorical_encode: "categorical_encode",
  numeric_scale: "numeric_scale",
  embedding: "embedding",
  feature_selection: "feature_selection",
  leakage_detect: "leakage_detect",
  quality_profile: "quality_profile",
  drift_compare: "drift_compare",
  determinism_profile: "determinism_profile",
  fit_state_registry: "fit_state_registry",
  pii_guard: "pii_guard",
  inference_parity: "inference_parity",
  split: "split",
  quality_gate: "quality_gate",
  ml_contract: "ml_contract",
  sql: "sql",
  json_to_table: "json_to_table",
  text_to_table: "text_to_table",
  table_to_json: "table_to_json",
};

const FLAT_FIELDS_BY_OP: Record<string, string[]> = {
  filter: ["expr"],
  select: ["mode", "columns", "keepOrder", "strict"],
  rename: ["map"],
  derive: ["columns"],
  aggregate: ["groupBy", "metrics"],
  join: ["clauses"],
  sort: ["by"],
  limit: ["n"],
  dedupe: ["allColumns", "by"],
  null_policy: ["mode", "columns", "fillValue", "stat", "rules"],
  outlier_policy: ["mode", "method", "columns", "iqrMultiplier", "zscoreThreshold", "lowerQuantile", "upperQuantile"],
  text_clean: ["columns", "lowercase", "unicodeNormalize", "removePunctuation", "removeUrls", "removeEmails", "removeEmoji", "normalizeWhitespace"],
  nlp_normalize: ["columns", "language", "removeStopwords", "stemmer", "lemmatizer", "tokenPattern"],
  tokenize_chunk: ["columns", "tokenizer", "tokenPattern", "maxTokens", "overlap", "sentenceAware", "outColumn"],
  dataset_split: ["strategy", "trainRatio", "valRatio", "testRatio", "seed", "shuffle", "stratifyColumn", "groupColumn", "timeColumn", "leakageGuard"],
  class_imbalance: ["strategy", "labelColumn", "targetRatio", "seed"],
  categorical_encode: ["columns", "encoding", "unknownPolicy", "rareThreshold", "dropFirst"],
  numeric_scale: ["columns", "method", "withCenter", "withScale", "clip", "clipMin", "clipMax"],
  embedding: ["columns", "provider", "model", "dimensions", "batchSize", "cacheEmbeddings", "outputColumn"],
  feature_selection: ["method", "columns", "topK", "varianceThreshold", "targetColumn", "selectedColumns"],
  leakage_detect: ["splitColumn", "keyColumns", "labelColumn", "maxAllowedOverlap"],
  quality_profile: ["columns", "includeHistograms", "includeSamples"],
  drift_compare: ["baselineRef", "compareColumns", "metric", "threshold", "failOnDrift"],
  determinism_profile: ["strict", "seed", "stableSort", "stableCoercion"],
  fit_state_registry: ["mode", "stateKey", "includeColumns"],
  pii_guard: ["columns", "action", "failOnDetect"],
  inference_parity: ["trainSignature", "inferenceSignature", "failOnMismatch"],
  split: [
    "sourceColumn",
    "outColumn",
    "mode",
    "lineBreak",
    "pattern",
    "delimiter",
    "flags",
    "trim",
    "dropEmpty",
    "emitIndex",
    "emitSourceRow",
    "maxParts",
  ],
  quality_gate: ["checks", "stopOnFail"],
  ml_contract: [
    "taskType",
    "labelColumn",
    "featureColumns",
    "idColumn",
    "timestampColumn",
    "allowExtraFeatures",
    "requireNonNullLabel",
  ],
  sql: ["dialect", "query"],
  json_to_table: ["orient", "rowsKey"],
  text_to_table: ["mode", "column", "delimiter", "hasHeader"],
  table_to_json: ["orient", "pretty"],
};

function normalizeTransformPatch(
  transformKind: string | undefined,
  existing: unknown,
  patch: unknown
): unknown {
  if (!isObject(patch)) return patch;

  const out: Record<string, unknown> = { ...patch };
  const ex = isObject(existing) ? existing : {};
  const op = String(out.op ?? ex.op ?? transformKind ?? "filter");
  const blockKey = OP_TO_BLOCK[op];
  if (!blockKey) return patch;

  out.op = op;
  const flatFields = FLAT_FIELDS_BY_OP[op] ?? [];
  const hadFlat = flatFields.some((f) => f in out);

  if (hadFlat) {
    const priorBlock = isObject(ex[blockKey]) ? (ex[blockKey] as Record<string, unknown>) : {};
    const nextBlock: Record<string, unknown> = { ...priorBlock };
    for (const f of flatFields) {
      if (f in out) {
        nextBlock[f] = out[f];
        delete out[f];
      }
    }
    out[blockKey] = nextBlock;
  }

  return out;
}

function patchIncludesRenameMap(patch: unknown): boolean {
  if (!isObject(patch)) return false;
  const renameBlock = patch.rename;
  if (isObject(renameBlock) && isObject(renameBlock.map)) return true;
  if (String(patch.op ?? "") === "rename" && isObject((patch as Record<string, unknown>).map)) return true;
  return false;
}

function normalizeExistingForTransformPatch(existing: unknown, patch: unknown): unknown {
  if (!isObject(existing) || !isObject(patch)) return existing;
  if (!patchIncludesRenameMap(patch)) return existing;

  const next: Record<string, unknown> = { ...existing };
  const renameBlock = isObject(next.rename) ? { ...(next.rename as Record<string, unknown>) } : {};
  renameBlock.map = {};
  next.rename = renameBlock;
  return next;
}

function patchIncludesBuiltinArgs(patch: unknown): boolean {
  if (!isObject(patch)) return false;
  const builtin = patch.builtin;
  if (!isObject(builtin)) return false;
  return isObject(builtin.args);
}

function normalizeExistingForToolBuiltinPatch(existing: unknown, patch: unknown): unknown {
  if (!isObject(existing) || !isObject(patch)) return existing;
  if (!patchIncludesBuiltinArgs(patch)) return existing;

  const next: Record<string, unknown> = { ...existing };
  const existingBuiltin = isObject(next.builtin) ? (next.builtin as Record<string, unknown>) : {};
  next.builtin = {
    ...existingBuiltin,
    args: {}
  };
  return next;
}

export function updateNodeParamsValidated(
  nodes: Node<PipelineNodeData>[],
  nodeId: string,
  patch: unknown
): { nodes: Node<PipelineNodeData>[]; error?: string } {
  const node = nodes.find((n) => n.id === nodeId);
  if (!node) return { nodes, error: "Node not found" };

  const existing = node.data.params;
  const normalizedPatch =
    node.data.kind === "transform"
      ? normalizeTransformPatch((node.data as any).transformKind, existing, patch)
      : patch;
  const existingForMerge =
    node.data.kind === "transform"
      ? normalizeExistingForTransformPatch(existing, normalizedPatch)
      : node.data.kind === "tool"
        ? normalizeExistingForToolBuiltinPatch(existing, normalizedPatch)
      : existing;
  const pick = pickValidation(node.data, normalizedPatch, existingForMerge);
  const defaultsForMerge =
    node.data.kind === "transform" && patchIncludesRenameMap(normalizedPatch) && isObject(pick.defaults)
      ? (() => {
          const nextDefaults = { ...(pick.defaults as Record<string, unknown>) };
          const renameBlock = isObject(nextDefaults.rename)
            ? { ...(nextDefaults.rename as Record<string, unknown>) }
            : {};
          renameBlock.map = {};
          nextDefaults.rename = renameBlock;
          return nextDefaults;
        })()
      : pick.defaults;

  const norm = normalizeWithDefaults(
    pick.schema as any,
    defaultsForMerge as any,
    existingForMerge,
    normalizedPatch
  );

  if (norm.ok === false) {
    return { nodes, error: norm.error };
  }

  const next = nodes.map((n) =>
    n.id === nodeId
      ? {
        ...n,
        data: {
          ...n.data,
          params: norm.value,
          meta: { ...n.data.meta, updatedAt: new Date().toISOString() }
        }
      }
      : n
  );

  return { nodes: next };
}
