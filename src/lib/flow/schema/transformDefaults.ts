// src/lib/flow/schema/transformDefaults.ts
import type { z } from "zod";
import { TransformParamsSchema } from "$lib/flow/schema/transform";

export type TransformParams = z.infer<typeof TransformParamsSchema>;

/**
 * Canonical default params for a Transform node.
 * Keep this file dumb: stable defaults only.
 */
export const defaultTransformParams: TransformParams = {
    op: "filter",
    enabled: true,
    notes: "",

    filter: { expr: "colA > 10" },

    // leave other op payloads undefined until op changes
    select: undefined,
    rename: undefined,
    derive: undefined,
    aggregate: undefined,
    join: undefined,
    sort: undefined,
    limit: undefined,
    dedupe: undefined,
    sql: undefined,
    code: undefined,

    cache: { enabled: false, key: undefined }
};

/**
 * Canonical default node.data for kind="transform"
 */
export const defaultTransformNodeData = {
    kind: "transform" as const,
    label: "Transform",
    params: defaultTransformParams,
    status: "idle" as const,
    ports: { in: "table" as const, out: "table" as const }
} as const;
