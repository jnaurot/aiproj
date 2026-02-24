// // src/lib/flow/schema/transformDefaults.ts
import type {
    TransformParams,
    TransformFilterParams,
    TransformSelectParams,
    TransformRenameParams,
    TransformDeriveParams,
    TransformAggregateParams,
    TransformJoinParams,
    TransformSortParams,
    TransformLimitParams,
    TransformDedupeParams,
    TransformSqlParams,
    TransformPythonParams
} from "$lib/flow/schema/transform"

// ─────────────────────────────────────────────
// Default params – ready to use when user picks an operation
// ─────────────────────────────────────────────

export const defaultTransformFilterParams: TransformFilterParams = {
    expr: "length(text) > 10",           // common starter: keep rows where text is long enough
};

export const defaultTransformSelectParams: TransformSelectParams = {
    columns: ["text", "id"],             // assume common columns; safe even if some don't exist (DuckDB will error gracefully)
};

export const defaultTransformRenameParams: TransformRenameParams = {
    map: {
        text: "description",               // very frequent rename when source is generic "text"
        column0: "value",
    },
};

export const defaultTransformDeriveParams: TransformDeriveParams = {
    columns: [
        {
            name: "length_text",
            expr: "length(text)",            // simple derived column example
        },
        {
            name: "is_long",
            expr: "length(text) > 50",
        },
    ],
};

export const defaultTransformAggregateParams: TransformAggregateParams = {
    groupBy: ["category"],               // assume a grouping column exists; empty = whole-table aggregate
    metrics: [
        {
            as: "row_count",
            expr: "count(*)",
        },
        {
            as: "avg_length",
            expr: "avg(length(text))",
        },
    ],
};

export const defaultTransformJoinParams: TransformJoinParams = {
    withNodeId: "",                      // must be filled by UI (node picker), but default empty string is valid for schema
    how: "inner",
    on: [
        {
            left: "id",
            right: "id",
        },
    ],
};

export const defaultTransformSortParams: TransformSortParams = {
    by: [
        {
            col: "text",
            dir: "asc",
        },
    ],
};

export const defaultTransformLimitParams: TransformLimitParams = {
    n: 100,                              // reasonable default: show first 100 rows
};

export const defaultTransformDedupeParams: TransformDedupeParams = {
    by: ["text"],                        // deduplicate on the main text column
    // by: [] would also be valid → dedupe on all columns
};

export const defaultTransformSqlParams: TransformSqlParams = {
    dialect: "duckdb",
    query: "SELECT * FROM input LIMIT 10",  // safe starter query; references registered table
};

export const defaultTransformPythonParams: TransformPythonParams = {
    source: `# Python transform example
# df is the input pandas DataFrame (DuckDB → pandas conversion happens automatically)
# return a pandas DataFrame

def transform(df):
    df['length'] = df['text'].str.len()
    return df[df['length'] > 20]
`,
    language: "python",
};

export const defaultTransformParamsByKind = {
    filter: {
        op: "filter",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        filter: defaultTransformFilterParams
    },
    select: {
        op: "select",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        select: defaultTransformSelectParams
    },
    rename: {
        op: "rename",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        rename: defaultTransformRenameParams
    },
    derive: {
        op: "derive",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        derive: defaultTransformDeriveParams
    },
    aggregate: {
        op: "aggregate",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        aggregate: defaultTransformAggregateParams
    },
    join: {
        op: "join",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        join: defaultTransformJoinParams
    },
    sort: {
        op: "sort",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        sort: defaultTransformSortParams
    },
    limit: {
        op: "limit",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        limit: defaultTransformLimitParams
    },
    dedupe: {
        op: "dedupe",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        dedupe: defaultTransformDedupeParams
    },
    sql: {
        op: "sql",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        sql: defaultTransformSqlParams
    },
    python: {
        op: "python",
        enabled: true,
        notes: "",
        cache: { enabled: false },
        code: defaultTransformPythonParams
    }
} as const;

export const defaultTransformParams: TransformParams = {
    op: "filter",
    enabled: true,
    notes: "",
    cache: { enabled: false },
    filter: { expr: "length(text) > 10" }
};

export const defaultTransformNodeData = {
    kind: "transform" as const,
    transformKind: "filter" as const,
    label: "Transform",
    params: defaultTransformParams,
    status: "idle" as const,
    ports: { in: "table", out: "table" as const },
};
