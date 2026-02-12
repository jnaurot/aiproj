// src/lib/flow/types/transform.ts
import type { BaseNodeData } from "./base";

export type TransformOp =
  | "filter"
  | "select"
  | "rename"
  | "derive"
  | "aggregate"
  | "join"
  | "sort"
  | "limit"
  | "dedupe"
  | "sql"
  | "python"
  | "js";

export type TransformCacheParams = { enabled: boolean; key?: string };

type Common = {
  enabled?: boolean;
  notes?: string;
  cache?: TransformCacheParams;
};

// ---- op-specific payloads ----

export type TransformParamsFilter = Common & {
  op: "filter";
  filter: { expr: string };
};

export type TransformParamsSelect = Common & {
  op: "select";
  select: { columns: string[] };
};

export type TransformParamsRename = Common & {
  op: "rename";
  rename: { map: Record<string, string> };
};

export type TransformParamsDerive = Common & {
  op: "derive";
  derive: { columns: { name: string; expr: string }[] };
};

export type TransformParamsAggregate = Common & {
  op: "aggregate";
  aggregate: {
    groupBy: string[];
    metrics: { as: string; expr: string }[];
  };
};

export type TransformParamsJoin = Common & {
  op: "join";
  join: {
    withNodeId: string;
    how: "inner" | "left" | "right" | "full";
    on: { left: string; right: string }[];
  };
};

export type TransformParamsSort = Common & {
  op: "sort";
  sort: { by: { col: string; dir: "asc" | "desc" }[] };
};

export type TransformParamsLimit = Common & {
  op: "limit";
  limit: { n: number };
};

export type TransformParamsDedupe = Common & {
  op: "dedupe";
  dedupe: { by?: string[] };
};

export type TransformParamsSql = Common & {
  op: "sql";
  sql: { dialect?: "duckdb" | "postgres" | "sqlite"; query: string };
};

export type TransformParamsPython = Common & {
  op: "python";
  code: { language: "python"; source: string };
};

export type TransformParamsJs = Common & {
  op: "js";
  code: { language: "js"; source: string };
};

// discriminated union: exactly one shape based on op
export type TransformParams =
  | TransformParamsFilter
  | TransformParamsSelect
  | TransformParamsRename
  | TransformParamsDerive
  | TransformParamsAggregate
  | TransformParamsJoin
  | TransformParamsSort
  | TransformParamsLimit
  | TransformParamsDedupe
  | TransformParamsSql
  | TransformParamsPython
  | TransformParamsJs;

export type TransformNodeData = BaseNodeData<"transform", TransformParams>;
