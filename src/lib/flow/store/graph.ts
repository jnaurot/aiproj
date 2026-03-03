import type { Node } from "@xyflow/svelte";
import type { PipelineNodeData } from "$lib/flow/types";
import { normalizeWithDefaults } from "$lib/flow/util/normalize";
import { defaultSourceParamsByKind } from "$lib/flow/schema/sourceDefaults";
import { SourceParamsSchemaByKind } from "$lib/flow/schema/source";
import { defaultLlmParamsByKind } from "$lib/flow/schema/llmDefaults";
import { LlmParamsSchemaByKind } from "$lib/flow/schema/llm";
import { defaultTransformParamsByKind } from "$lib/flow/schema/transformDefaults"
import { TransformParamsSchema } from "$lib/flow/schema/transform"
import { ToolParamsSchema } from "$lib/flow/schema/tool";
import { defaultToolParamsByProvider } from "$lib/flow/schema/toolDefaults";

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

    case "llm": {
      const lk = data.llmKind ?? "ollama"
      return {
        schema: LlmParamsSchemaByKind[lk],
        defaults: defaultLlmParamsByKind[lk]
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
  sql: "sql",
};

const FLAT_FIELDS_BY_OP: Record<string, string[]> = {
  filter: ["expr"],
  select: ["columns"],
  rename: ["map"],
  derive: ["columns"],
  aggregate: ["groupBy", "metrics"],
  join: ["withNodeId", "how", "on"],
  sort: ["by"],
  limit: ["n"],
  dedupe: ["by"],
  sql: ["dialect", "query"],
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
  const pick = pickValidation(node.data, normalizedPatch, existing);

  const norm = normalizeWithDefaults(
    pick.schema as any,
    pick.defaults as any,
    existing,
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
