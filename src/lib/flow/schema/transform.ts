//src/lib/flow/schema/transform.ts
import { z } from "zod";
import { NodeDebugParamsSchema } from "./debug";

// ---- shared enums ----
const TransformKindSchema = z.enum(["filter",
  "json_filter",
  "select",
  "rename",
  "derive",
  "aggregate",
  "join",
  "sort",
  "limit",
  "dedupe",
  "null_policy",
  "outlier_policy",
  "text_clean",
  "nlp_normalize",
  "tokenize_chunk",
  "dataset_split",
  "class_imbalance",
  "categorical_encode",
  "numeric_scale",
  "embedding",
  "feature_selection",
  "leakage_detect",
  "quality_profile",
  "drift_compare",
  "determinism_profile",
  "fit_state_registry",
  "pii_guard",
  "inference_parity",
  "split",
  "quality_gate",
  "ml_contract",
  "sql",
  "json_to_table",
  "text_to_table",
  "table_to_json"]);

// ─────────────────────────────────────────────
// Per-operation parameter schemas
// Each is standalone, strict, and uses .strip() to reject unknown keys
// ─────────────────────────────────────────────

const TransformFilterModeSchema = z.enum(["rules", "sql"]);
const TransformFilterOperatorSchema = z.enum([
	"eq",
	"ne",
	"gt",
	"gte",
	"lt",
	"lte",
	"contains",
	"in",
	"not_in",
	"regex",
	"exists",
	"between",
	"is_null",
	"not_null"
]);
const TransformFilterGroupOperatorSchema = z.enum(["all", "any"]);

const TransformFilterLiteralValueSchema = z.union([
	z.string(),
	z.number(),
	z.boolean(),
	z.null(),
	z.array(z.union([z.string(), z.number(), z.boolean(), z.null()]))
]);

const TransformFilterValueFromSchema = z.object({
	handle: z.literal("param_config").default("param_config"),
	path: z.string().regex(
		/^([A-Za-z_][A-Za-z0-9_]*)(\.[A-Za-z_][A-Za-z0-9_]*)*$/,
		"Path must use dot notation, for example: location.country"
	)
}).strip();

const TransformFilterValueSchema = z.union([
	TransformFilterLiteralValueSchema,
	z.object({
		valueFrom: TransformFilterValueFromSchema
	}).strip()
]);

const TransformFilterConditionSchema = z.object({
	kind: z.literal("condition").default("condition"),
	column: z.string().min(1, "Filter condition column cannot be empty"),
	op: TransformFilterOperatorSchema,
	value: TransformFilterValueSchema.optional()
}).superRefine((val, ctx) => {
	const opsRequiringValue = new Set([
		"eq",
		"ne",
		"gt",
		"gte",
		"lt",
		"lte",
		"contains",
		"in",
		"not_in",
		"regex",
		"between"
	]);
	const opsNoValue = new Set(["exists", "is_null", "not_null"]);
	if (opsRequiringValue.has(val.op) && val.value === undefined) {
		ctx.addIssue({
			code: "custom",
			path: ["value"],
			message: `Operator '${val.op}' requires a value`
		});
	}
	if (opsNoValue.has(val.op) && val.value !== undefined) {
		ctx.addIssue({
			code: "custom",
			path: ["value"],
			message: `Operator '${val.op}' must not provide a value`
		});
	}
}).strip();

type TransformFilterRuleNode = z.infer<typeof TransformFilterConditionSchema> | {
	kind: "group";
	op: z.infer<typeof TransformFilterGroupOperatorSchema>;
	conditions: TransformFilterRuleNode[];
};

const TransformFilterRuleNodeSchema: z.ZodType<TransformFilterRuleNode> = z.lazy(() => z.union([
	TransformFilterConditionSchema,
	z.object({
		kind: z.literal("group").default("group"),
		op: TransformFilterGroupOperatorSchema.default("all"),
		conditions: z.array(TransformFilterRuleNodeSchema).default([])
	}).strip()
]));

export const TransformFilterRuleGroupSchema = z.object({
	kind: z.literal("group").default("group"),
	op: TransformFilterGroupOperatorSchema.default("all"),
	conditions: z.array(TransformFilterRuleNodeSchema).default([])
}).strip();

const TransformFilterParamsSchemaBase = z.object({
	mode: TransformFilterModeSchema.default("rules"),
	expr: z.string().default(""),
	rules: TransformFilterRuleGroupSchema.optional()
}).strip();

export const TransformFilterParamsSchema = z.preprocess((raw) => {
	if (!raw || typeof raw !== "object" || Array.isArray(raw)) return raw;
	const record = raw as Record<string, unknown>;
	const hasMode = typeof record.mode === "string" && String(record.mode).trim().length > 0;
	const expr = typeof record.expr === "string" ? record.expr.trim() : "";
	if (!hasMode && expr.length > 0) {
		return { ...record, mode: "sql" };
	}
	return raw;
}, TransformFilterParamsSchemaBase);

const TransformJsonFilterOperatorSchema = z.enum([
	"eq",
	"ne",
	"gt",
	"gte",
	"lt",
	"lte",
	"in",
	"contains",
	"exists",
	"is_null",
	"between"
]);

const TransformJsonFilterConditionSchema = z
	.object({
		kind: z.literal("condition").default("condition"),
		path: z.string().optional(),
		column: z.string().optional(),
		op: TransformJsonFilterOperatorSchema,
		value: TransformFilterValueSchema.optional()
	})
	.superRefine((val, ctx) => {
		const path = String(val.path ?? val.column ?? '').trim();
		if (!path) {
			ctx.addIssue({
				code: 'custom',
				path: ['path'],
				message: 'JSON filter condition path is required'
			});
		}
		const needsValue = new Set(['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'contains', 'between']);
		const noValue = new Set(['exists', 'is_null']);
		if (needsValue.has(val.op) && val.value === undefined) {
			ctx.addIssue({
				code: 'custom',
				path: ['value'],
				message: `Operator '${val.op}' requires a value`
			});
		}
		if (noValue.has(val.op) && val.value !== undefined) {
			ctx.addIssue({
				code: 'custom',
				path: ['value'],
				message: `Operator '${val.op}' must not provide a value`
			});
		}
	})
	.strip();

type TransformJsonFilterRuleNode =
	| z.infer<typeof TransformJsonFilterConditionSchema>
	| {
			kind: 'group';
			op: z.infer<typeof TransformFilterGroupOperatorSchema>;
			conditions: TransformJsonFilterRuleNode[];
	  };

const TransformJsonFilterRuleNodeSchema: z.ZodType<TransformJsonFilterRuleNode> = z.lazy(() =>
	z.union([
		TransformJsonFilterConditionSchema,
		z
			.object({
				kind: z.literal('group').default('group'),
				op: TransformFilterGroupOperatorSchema.default('all'),
				conditions: z.array(TransformJsonFilterRuleNodeSchema).default([])
			})
			.strip()
	])
);

export const TransformJsonFilterRuleGroupSchema = z
	.object({
		kind: z.literal('group').default('group'),
		op: TransformFilterGroupOperatorSchema.default('all'),
		conditions: z.array(TransformJsonFilterRuleNodeSchema).default([])
	})
	.strip();

export const TransformJsonFilterParamsSchema = z
	.object({
		mode: z.literal('rules').default('rules'),
		rules: TransformJsonFilterRuleGroupSchema.default({
			kind: 'group',
			op: 'all',
			conditions: []
		}),
		route_reject: z.boolean().default(true),
		include_reject_meta: z.boolean().default(true)
	})
	.strip();

export const TransformSelectParamsSchema = z.object({
	mode: z.enum(["include", "exclude"]).default("include"),
	columns: z.array(z.string().min(1)).default([]),
	keepOrder: z.enum(["input", "custom"]).optional(),
	strict: z.boolean().default(true),
}).superRefine((val, ctx) => {
	const seen = new Set<string>();
	for (let i = 0; i < val.columns.length; i += 1) {
		const col = String(val.columns[i] ?? "").trim();
		if (!col) {
			ctx.addIssue({
				code: "custom",
				path: ["columns", i],
				message: "Column name cannot be empty",
			});
			continue;
		}
		if (seen.has(col)) {
			ctx.addIssue({
				code: "custom",
				path: ["columns"],
				message: `Duplicate selected column: ${col}`,
			});
			continue;
		}
		seen.add(col);
	}
}).strip();

export const TransformRenameParamsSchema = z.object({
  map: z.record(
    z.string().min(1),
    z.string().min(1, "New column name cannot be empty")
  ).refine(
    (map) => Object.keys(map).length > 0,
    { message: "Rename map cannot be empty" }
  ),
}).strip();

const TransformDeriveModeSchema = z.enum(["rules", "sql"]);
const TransformDeriveFormulaOpSchema = z.enum([
	"add",
	"sub",
	"mul",
	"div",
	"concat",
	"lower",
	"upper",
	"trim",
	"length",
	"coalesce"
]);

const TransformDeriveValueFromSchema = z.object({
	handle: z.literal("param_config").default("param_config"),
	path: z.string().regex(
		/^([A-Za-z_][A-Za-z0-9_]*)(\.[A-Za-z_][A-Za-z0-9_]*)*$/,
		"Path must use dot notation, for example: preferences.salary_min"
	)
}).strip();

const TransformDeriveLiteralArgSchema = z.union([
	z.string(),
	z.number(),
	z.boolean(),
	z.null()
]);

const TransformDeriveFormulaArgSchema = z.union([
	TransformDeriveLiteralArgSchema,
	z.object({
		column: z.string().min(1, "Formula column reference cannot be empty")
	}).strip(),
	z.object({
		valueFrom: TransformDeriveValueFromSchema
	}).strip()
]);

const TransformDeriveFormulaSchema = z.object({
	op: TransformDeriveFormulaOpSchema,
	args: z.array(TransformDeriveFormulaArgSchema).default([])
}).superRefine((val, ctx) => {
	const arity: Record<z.infer<typeof TransformDeriveFormulaOpSchema>, { min: number; max: number }> = {
		add: { min: 2, max: 2 },
		sub: { min: 2, max: 2 },
		mul: { min: 2, max: 2 },
		div: { min: 2, max: 2 },
		concat: { min: 2, max: Number.POSITIVE_INFINITY },
		lower: { min: 1, max: 1 },
		upper: { min: 1, max: 1 },
		trim: { min: 1, max: 1 },
		length: { min: 1, max: 1 },
		coalesce: { min: 1, max: Number.POSITIVE_INFINITY }
	};
	const bounds = arity[val.op];
	if (val.args.length < bounds.min) {
		ctx.addIssue({
			code: "custom",
			path: ["args"],
			message: `Formula op '${val.op}' requires at least ${bounds.min} argument(s)`
		});
	}
	if (val.args.length > bounds.max) {
		ctx.addIssue({
			code: "custom",
			path: ["args"],
			message: `Formula op '${val.op}' allows at most ${bounds.max} argument(s)`
		});
	}
}).strip();

const TransformDeriveSqlColumnSchema = z.object({
	name: z.string().min(1, "Derived column name cannot be empty"),
	expr: z.string().min(1, "Derived expression cannot be empty")
}).strip();

const TransformDeriveRuleColumnSchema = z.object({
	name: z.string().min(1, "Derived column name cannot be empty"),
	formula: TransformDeriveFormulaSchema
}).strip();

const TransformDeriveParamsSchemaBase = z.object({
	mode: TransformDeriveModeSchema.default("rules"),
	columns: z.array(TransformDeriveSqlColumnSchema).default([]),
	rules: z.array(TransformDeriveRuleColumnSchema).default([])
}).superRefine((val, ctx) => {
	if (val.mode === "sql" && val.columns.length === 0) {
		ctx.addIssue({
			code: "custom",
			path: ["columns"],
			message: "Derive SQL mode requires at least one derived column expression"
		});
	}
	if (val.mode === "rules" && val.rules.length === 0) {
		ctx.addIssue({
			code: "custom",
			path: ["rules"],
			message: "Derive rules mode requires at least one formula rule"
		});
	}
}).strip();

export const TransformDeriveParamsSchema = z.preprocess((raw) => {
	if (!raw || typeof raw !== "object" || Array.isArray(raw)) return raw;
	const record = raw as Record<string, unknown>;
	const hasMode = typeof record.mode === "string" && String(record.mode).trim().length > 0;
	const hasSqlColumns = Array.isArray(record.columns) && record.columns.some((entry) => {
		if (!entry || typeof entry !== "object" || Array.isArray(entry)) return false;
		const expr = (entry as Record<string, unknown>).expr;
		return typeof expr === "string" && expr.trim().length > 0;
	});
	if (!hasMode && hasSqlColumns) {
		return { ...record, mode: "sql" };
	}
	return raw;
}, TransformDeriveParamsSchemaBase);

export const TransformAggregateParamsSchema = z.object({
  groupBy: z.array(z.string().min(1)).optional().default([]),
  metrics: z.array(
    z
      .object({
        name: z.string().min(1, "Aggregate metric name cannot be empty"),
        op: z.enum([
          "count_rows",
          "count",
          "count_distinct",
          "min",
          "max",
          "sum",
          "mean",
          "avg_length",
          "min_length",
          "max_length",
        ]),
        column: z.string().min(1).nullable().optional(),
      })
      .strip()
  ).min(1, "Aggregate must specify at least one metric"),
}).superRefine((val, ctx) => {
  const needsColumn = new Set([
    "count",
    "count_distinct",
    "min",
    "max",
    "sum",
    "mean",
    "avg_length",
    "min_length",
    "max_length",
  ]);
  const seenNames = new Set<string>();
  for (let i = 0; i < val.metrics.length; i += 1) {
    const metric = val.metrics[i];
    const name = String(metric.name ?? "").trim();
    if (!name) {
      ctx.addIssue({
        code: "custom",
        path: ["metrics", i, "name"],
        message: "Aggregate metric name cannot be empty",
      });
      continue;
    }
    if (seenNames.has(name)) {
      ctx.addIssue({
        code: "custom",
        path: ["metrics", i, "name"],
        message: "Aggregate metric names must be unique",
      });
    } else {
      seenNames.add(name);
    }
    if (needsColumn.has(metric.op) && !String(metric.column ?? "").trim()) {
      ctx.addIssue({
        code: "custom",
        path: ["metrics", i, "column"],
        message: `Aggregate op '${metric.op}' requires a column`,
      });
    }
  }
}).strip();

export const TransformJoinParamsSchema = z.object({
  clauses: z
    .array(
      z.object({
        leftNodeId: z.string().min(1, "Left join node id cannot be empty"),
        leftCol: z.string().min(1, "Left join key cannot be empty"),
        rightNodeId: z.string().min(1, "Right join node id cannot be empty"),
        rightCol: z.string().min(1, "Right join key cannot be empty"),
        how: z.enum(["inner", "left", "right", "full"]).default("inner"),
      })
    )
    .min(1, "Join must specify at least one clause"),
}).strip();

export const TransformSortParamsSchema = z.object({
  by: z
    .array(
      z.object({
        col: z.string().min(1, "Sort column cannot be empty"),
        dir: z.enum(["asc", "desc"]),
      })
    )
    .min(1, "Sort must specify at least one column"),
}).strip();

export const TransformLimitParamsSchema = z.object({
  n: z.number()
    .int("Limit must be an integer")
    .nonnegative("Limit cannot be negative")
    .min(1, "Limit must be at least 1"),
}).strip();

// export const TransformDedupeParamsSchema = z.object({
//   allColumns: z.boolean().optional().default(false),
//   by: z.array(z.string().min(1)).optional().default([]),
// }).strip();

export const TransformDedupeParamsSchema = z.object({
  allColumns: z.boolean().default(false),
  by: z.array(z.string().min(1)).default([]),
}).superRefine((val, ctx) => {
  if (val.allColumns && val.by.length > 0) {
    ctx.addIssue({
      code: "custom",
      path: ["by"],
      message: "by must be empty when allColumns is true",
    });
  }
	if (!val.allColumns && val.by.length === 0) {
		ctx.addIssue({
			code: 'custom',
			path: ['by'],
			message: 'by must contain at least one column when allColumns is false'
		});
	}
}).strip();

const TransformNullPolicyModeSchema = z.enum([
	'report',
	'drop_rows',
	'fill_constant',
	'fill_stat'
]);

const TransformNullPolicyStatSchema = z.enum(['mean', 'median', 'mode']);

const TransformNullPolicyRuleSchema = z
	.object({
		column: z.string().min(1),
		mode: TransformNullPolicyModeSchema.optional(),
		fillValue: z.any().optional(),
		stat: TransformNullPolicyStatSchema.optional()
	})
	.strip();

export const TransformNullPolicyParamsSchema = z
	.object({
		mode: TransformNullPolicyModeSchema.default('report'),
		columns: z.array(z.string().min(1)).default([]),
		fillValue: z.any().optional(),
		stat: TransformNullPolicyStatSchema.default('mean'),
		rules: z.array(TransformNullPolicyRuleSchema).default([])
	})
	.strip();

const TransformOutlierPolicyModeSchema = z.enum(['clip', 'winsorize', 'drop']);
const TransformOutlierPolicyMethodSchema = z.enum(['iqr', 'zscore', 'quantile']);

export const TransformOutlierPolicyParamsSchema = z
	.object({
		mode: TransformOutlierPolicyModeSchema.default('clip'),
		method: TransformOutlierPolicyMethodSchema.default('iqr'),
		columns: z.array(z.string().min(1)).default([]),
		iqrMultiplier: z.number().positive().default(1.5),
		zscoreThreshold: z.number().positive().default(3),
		lowerQuantile: z.number().min(0).max(1).default(0.01),
		upperQuantile: z.number().min(0).max(1).default(0.99)
	})
	.superRefine((v, ctx) => {
		if (v.lowerQuantile >= v.upperQuantile) {
			ctx.addIssue({
				code: 'custom',
				path: ['upperQuantile'],
				message: 'upperQuantile must be greater than lowerQuantile'
			});
		}
	})
	.strip();

export const TransformTextCleanParamsSchema = z
	.object({
		columns: z.array(z.string().min(1)).default([]),
		lowercase: z.boolean().default(true),
		unicodeNormalize: z.enum(['none', 'nfc', 'nfkc']).default('nfkc'),
		removePunctuation: z.boolean().default(false),
		removeUrls: z.boolean().default(true),
		removeEmails: z.boolean().default(true),
		removeEmoji: z.boolean().default(false),
		normalizeWhitespace: z.boolean().default(true)
	})
	.strip();

export const TransformNlpNormalizeParamsSchema = z
	.object({
		columns: z.array(z.string().min(1)).default([]),
		language: z.string().min(2).default('en'),
		removeStopwords: z.boolean().default(true),
		stemmer: z.enum(['none', 'porter']).default('none'),
		lemmatizer: z.enum(['none', 'rule_based']).default('none'),
		tokenPattern: z.string().min(1).default('\\w+')
	})
	.strip();

export const TransformTokenizeChunkParamsSchema = z
	.object({
		columns: z.array(z.string().min(1)).default([]),
		tokenizer: z.enum(['whitespace', 'regex']).default('whitespace'),
		tokenPattern: z.string().min(1).default('\\w+'),
		maxTokens: z.number().int().min(1).max(100000).default(256),
		overlap: z.number().int().min(0).max(50000).default(32),
		sentenceAware: z.boolean().default(true),
		outColumn: z.string().min(1).default('chunk')
	})
	.superRefine((v, ctx) => {
		if (v.overlap >= v.maxTokens) {
			ctx.addIssue({
				code: 'custom',
				path: ['overlap'],
				message: 'overlap must be less than maxTokens'
			});
		}
	})
	.strip();

export const TransformDatasetSplitParamsSchema = z
	.object({
		strategy: z.enum(['random', 'stratified', 'group', 'time']).default('random'),
		trainRatio: z.number().min(0).max(1).default(0.8),
		valRatio: z.number().min(0).max(1).default(0.1),
		testRatio: z.number().min(0).max(1).default(0.1),
		seed: z.number().int().default(42),
		shuffle: z.boolean().default(true),
		stratifyColumn: z.string().default(''),
		groupColumn: z.string().default(''),
		timeColumn: z.string().default(''),
		leakageGuard: z.boolean().default(true)
	})
	.superRefine((v, ctx) => {
		const sum = Number(v.trainRatio) + Number(v.valRatio) + Number(v.testRatio);
		if (Math.abs(sum - 1) > 1e-6) {
			ctx.addIssue({
				code: 'custom',
				path: ['testRatio'],
				message: 'trainRatio + valRatio + testRatio must equal 1'
			});
		}
		if (v.strategy === 'stratified' && !String(v.stratifyColumn).trim()) {
			ctx.addIssue({
				code: 'custom',
				path: ['stratifyColumn'],
				message: 'stratifyColumn is required when strategy=stratified'
			});
		}
		if (v.strategy === 'group' && !String(v.groupColumn).trim()) {
			ctx.addIssue({
				code: 'custom',
				path: ['groupColumn'],
				message: 'groupColumn is required when strategy=group'
			});
		}
		if (v.strategy === 'time' && !String(v.timeColumn).trim()) {
			ctx.addIssue({
				code: 'custom',
				path: ['timeColumn'],
				message: 'timeColumn is required when strategy=time'
			});
		}
	})
	.strip();

export const TransformClassImbalanceParamsSchema = z
	.object({
		strategy: z.enum(['report', 'undersample', 'oversample', 'class_weight']).default('report'),
		labelColumn: z.string().min(1).default('label'),
		targetRatio: z.number().min(0).max(1).default(1),
		seed: z.number().int().default(42)
	})
	.strip();

export const TransformCategoricalEncodeParamsSchema = z
	.object({
		columns: z.array(z.string().min(1)).default([]),
		encoding: z.enum(['one_hot', 'ordinal', 'frequency']).default('one_hot'),
		unknownPolicy: z.enum(['ignore', 'error', 'impute']).default('ignore'),
		rareThreshold: z.number().min(0).max(1).default(0),
		dropFirst: z.boolean().default(false)
	})
	.strip();

export const TransformNumericScaleParamsSchema = z
	.object({
		columns: z.array(z.string().min(1)).default([]),
		method: z.enum(['standard', 'minmax', 'robust']).default('standard'),
		withCenter: z.boolean().default(true),
		withScale: z.boolean().default(true),
		clip: z.boolean().default(false),
		clipMin: z.number().optional(),
		clipMax: z.number().optional()
	})
	.superRefine((v, ctx) => {
		if (v.clip && v.clipMin !== undefined && v.clipMax !== undefined && v.clipMin > v.clipMax) {
			ctx.addIssue({
				code: 'custom',
				path: ['clipMax'],
				message: 'clipMax must be >= clipMin'
			});
		}
	})
	.strip();

export const TransformEmbeddingParamsSchema = z
	.object({
		columns: z.array(z.string().min(1)).default([]),
		provider: z.enum(['local_hash', 'openai', 'ollama']).default('local_hash'),
		model: z.string().min(1).default('text-embedding-3-small'),
		dimensions: z.number().int().min(1).max(4096).default(16),
		batchSize: z.number().int().min(1).max(2048).default(64),
		cacheEmbeddings: z.boolean().default(true),
		outputColumn: z.string().min(1).default('embedding')
	})
	.strip();

export const TransformFeatureSelectionParamsSchema = z
	.object({
		method: z.enum(['variance', 'mutual_info', 'model_importance', 'manual']).default('variance'),
		columns: z.array(z.string().min(1)).default([]),
		topK: z.number().int().min(1).default(50),
		varianceThreshold: z.number().min(0).default(0),
		targetColumn: z.string().min(1).default('label'),
		selectedColumns: z.array(z.string().min(1)).default([])
	})
	.strip();

export const TransformLeakageDetectParamsSchema = z
	.object({
		splitColumn: z.string().min(1).default('split'),
		keyColumns: z.array(z.string().min(1)).default([]),
		labelColumn: z.string().default(''),
		maxAllowedOverlap: z.number().min(0).max(1).default(0)
	})
	.strip();

export const TransformQualityProfileParamsSchema = z
	.object({
		columns: z.array(z.string().min(1)).default([]),
		includeHistograms: z.boolean().default(true),
		includeSamples: z.boolean().default(true)
	})
	.strip();

export const TransformDriftCompareParamsSchema = z
	.object({
		baselineRef: z.string().default(''),
		compareColumns: z.array(z.string().min(1)).default([]),
		metric: z.enum(['psi', 'jsd', 'ks']).default('psi'),
		threshold: z.number().min(0).default(0.2),
		failOnDrift: z.boolean().default(false)
	})
	.strip();

export const TransformDeterminismProfileParamsSchema = z
	.object({
		strict: z.boolean().default(true),
		seed: z.number().int().default(42),
		stableSort: z.boolean().default(true),
		stableCoercion: z.boolean().default(true)
	})
	.strip();

export const TransformFitStateRegistryParamsSchema = z
	.object({
		mode: z.enum(['fit', 'apply']).default('fit'),
		stateKey: z.string().min(1).default('default'),
		includeColumns: z.array(z.string().min(1)).default([])
	})
	.strip();

export const TransformPiiGuardParamsSchema = z
	.object({
		columns: z.array(z.string().min(1)).default([]),
		action: z.enum(['report', 'mask', 'drop_rows']).default('report'),
		failOnDetect: z.boolean().default(false)
	})
	.strip();

export const TransformInferenceParityParamsSchema = z
	.object({
		trainSignature: z.string().default(''),
		inferenceSignature: z.string().default(''),
		failOnMismatch: z.boolean().default(true)
	})
	.strip();

export const TransformSqlParamsSchema = z.object({
  dialect: z.enum(["duckdb", "postgres", "sqlite"]).optional().default("duckdb"),
  query: z.string().min(1, "SQL query cannot be empty"),
  max_runtime_ms: z.coerce.number().int().min(0).default(0),
  max_output_rows: z.coerce.number().int().min(0).default(0),
  safe_mode: z.boolean().default(true),
  declared_output_columns: z
    .array(
      z
        .object({
          name: z.string().min(1, "Declared output column name cannot be empty"),
          type: z.string().min(1).optional().default("unknown"),
          nullable: z.boolean().optional().default(true)
        })
        .strip()
    )
    .optional()
    .default([]),
}).strip();

export const TransformJsonToTableParamsSchema = z
  .object({
    orient: z.enum(["records", "object"]).default("records"),
    rowsKey: z.string().min(1).default("rows")
  })
  .strip();

export const TransformTextToTableParamsSchema = z
  .object({
    mode: z.enum(["lines", "csv", "tsv"]).default("lines"),
    column: z.string().min(1).default("text"),
    delimiter: z.string().optional().default(","),
    hasHeader: z.boolean().default(true),
  })
  .strip();

export const TransformTableToJsonParamsSchema = z
  .object({
    orient: z.enum(["records", "split"]).default("records"),
    pretty: z.boolean().default(false),
  })
  .strip();

export const TransformSplitParamsSchema = z
	.object({
		sourceColumn: z.string().min(1, 'Source column is required').default('text'),
		outColumn: z.string().min(1, 'Output column is required').default('part'),
		mode: z.enum(['sentences', 'lines', 'regex', 'delimiter']).default('sentences'),
		lineBreak: z.enum(['any', 'lf', 'crlf', 'cr']).optional().default('any'),
		pattern: z.string().optional(),
		delimiter: z.string().optional(),
		flags: z
			.string()
			.default('')
			.refine((v) => /^[ims]*$/.test(v), 'Flags must contain only i, m, s'),
		trim: z.boolean().default(true),
		dropEmpty: z.boolean().default(true),
		emitIndex: z.boolean().default(true),
		emitSourceRow: z.boolean().default(true),
		maxParts: z.number().int().min(1).max(100000).default(5000)
	})
	.superRefine((v, ctx) => {
		if (v.mode === 'regex' && !String(v.pattern ?? '').trim()) {
			ctx.addIssue({
				code: 'custom',
				path: ['pattern'],
				message: 'Pattern is required when mode=regex'
			});
		}
		if (v.mode === 'delimiter' && !String(v.delimiter ?? '').length) {
			ctx.addIssue({
				code: 'custom',
				path: ['delimiter'],
				message: 'Delimiter is required when mode=delimiter'
			});
		}
	})
	.strip();

const TransformQualityGateSeveritySchema = z.enum(['warn', 'fail']).default('fail');

export const TransformQualityGateCheckSchema = z
	.discriminatedUnion('kind', [
		z
			.object({
				kind: z.literal('null_pct'),
				column: z.string().min(1),
				maxNullPct: z.number().min(0).max(1).default(0),
				severity: TransformQualityGateSeveritySchema
			})
			.strip(),
		z
			.object({
				kind: z.literal('range'),
				column: z.string().min(1),
				min: z.number().optional(),
				max: z.number().optional(),
				inclusiveMin: z.boolean().default(true),
				inclusiveMax: z.boolean().default(true),
				maxOutOfRangePct: z.number().min(0).max(1).default(0),
				severity: TransformQualityGateSeveritySchema
			})
			.superRefine((v, ctx) => {
				if (v.min === undefined && v.max === undefined) {
					ctx.addIssue({
						code: 'custom',
						path: ['min'],
						message: 'Range check requires min and/or max'
					});
				}
			})
			.strip(),
		z
			.object({
				kind: z.literal('uniqueness'),
				column: z.string().min(1),
				minUniqueRatio: z.number().min(0).max(1).default(1),
				severity: TransformQualityGateSeveritySchema
			})
			.strip(),
		z
			.object({
				kind: z.literal('class_balance'),
				column: z.string().min(1),
				minMinorityRatio: z.number().min(0).max(1).default(0),
				maxDominantRatio: z.number().min(0).max(1).default(1),
				severity: TransformQualityGateSeveritySchema
			})
			.strip(),
		z
			.object({
				kind: z.literal('leakage'),
				featureColumn: z.string().min(1),
				targetColumn: z.string().min(1),
				maxAbsCorrelation: z.number().min(0).max(1).default(0.95),
				severity: TransformQualityGateSeveritySchema
			})
			.strip()
	]);

export const TransformQualityGateParamsSchema = z
	.object({
		checks: z.array(TransformQualityGateCheckSchema).default([]),
		stopOnFail: z.boolean().default(true)
	})
	.strip();

export const TransformMlContractParamsSchema = z
	.object({
		taskType: z
			.enum(['classification', 'regression', 'ranking', 'generation', 'embedding', 'pretraining', 'finetuning', 'other'])
			.default('other'),
		labelColumn: z.string().min(1).default('label'),
		featureColumns: z.array(z.string().min(1)).min(1).default(['text']),
		idColumn: z.string().optional().default(''),
		timestampColumn: z.string().optional().default(''),
		allowExtraFeatures: z.boolean().default(true),
		requireNonNullLabel: z.boolean().default(true)
	})
	.strip();

export const TransformParamsSchemaByKind = {
  filter: TransformFilterParamsSchema,
  json_filter: TransformJsonFilterParamsSchema,
  select: TransformSelectParamsSchema,
  rename: TransformRenameParamsSchema,
  derive: TransformDeriveParamsSchema,
  aggregate: TransformAggregateParamsSchema,
  join: TransformJoinParamsSchema,
  sort: TransformSortParamsSchema,
  limit: TransformLimitParamsSchema,
  dedupe: TransformDedupeParamsSchema,
  null_policy: TransformNullPolicyParamsSchema,
  outlier_policy: TransformOutlierPolicyParamsSchema,
  text_clean: TransformTextCleanParamsSchema,
  nlp_normalize: TransformNlpNormalizeParamsSchema,
  tokenize_chunk: TransformTokenizeChunkParamsSchema,
  dataset_split: TransformDatasetSplitParamsSchema,
  class_imbalance: TransformClassImbalanceParamsSchema,
  categorical_encode: TransformCategoricalEncodeParamsSchema,
  numeric_scale: TransformNumericScaleParamsSchema,
  embedding: TransformEmbeddingParamsSchema,
  feature_selection: TransformFeatureSelectionParamsSchema,
  leakage_detect: TransformLeakageDetectParamsSchema,
  quality_profile: TransformQualityProfileParamsSchema,
  drift_compare: TransformDriftCompareParamsSchema,
  determinism_profile: TransformDeterminismProfileParamsSchema,
  fit_state_registry: TransformFitStateRegistryParamsSchema,
  pii_guard: TransformPiiGuardParamsSchema,
  inference_parity: TransformInferenceParityParamsSchema,
  split: TransformSplitParamsSchema,
  quality_gate: TransformQualityGateParamsSchema,
  ml_contract: TransformMlContractParamsSchema,
  sql: TransformSqlParamsSchema,
  json_to_table: TransformJsonToTableParamsSchema,
  text_to_table: TransformTextToTableParamsSchema,
  table_to_json: TransformTableToJsonParamsSchema,
} as const

// ---- inferred types (single source of truth) ----
export type TransformFilterParams = z.infer<typeof TransformFilterParamsSchema>;
export type TransformJsonFilterParams = z.infer<typeof TransformJsonFilterParamsSchema>;
export type TransformSelectParams  = z.infer<typeof   TransformSelectParamsSchema>;
export type TransformRenameParams  = z.infer<typeof   TransformRenameParamsSchema>;
export type TransformDeriveParams  = z.infer<typeof   TransformDeriveParamsSchema>;
export type TransformAggregateParams  = z.infer<typeof   TransformAggregateParamsSchema>;
export type TransformJoinParams  = z.infer<typeof   TransformJoinParamsSchema>;
export type TransformSortParams  = z.infer<typeof   TransformSortParamsSchema>;
export type TransformLimitParams  = z.infer<typeof   TransformLimitParamsSchema>;
export type  TransformDedupeParams = z.infer<typeof   TransformDedupeParamsSchema>;
export type TransformNullPolicyParams = z.infer<typeof TransformNullPolicyParamsSchema>;
export type TransformOutlierPolicyParams = z.infer<typeof TransformOutlierPolicyParamsSchema>;
export type TransformTextCleanParams = z.infer<typeof TransformTextCleanParamsSchema>;
export type TransformNlpNormalizeParams = z.infer<typeof TransformNlpNormalizeParamsSchema>;
export type TransformTokenizeChunkParams = z.infer<typeof TransformTokenizeChunkParamsSchema>;
export type TransformDatasetSplitParams = z.infer<typeof TransformDatasetSplitParamsSchema>;
export type TransformClassImbalanceParams = z.infer<typeof TransformClassImbalanceParamsSchema>;
export type TransformCategoricalEncodeParams = z.infer<typeof TransformCategoricalEncodeParamsSchema>;
export type TransformNumericScaleParams = z.infer<typeof TransformNumericScaleParamsSchema>;
export type TransformEmbeddingParams = z.infer<typeof TransformEmbeddingParamsSchema>;
export type TransformFeatureSelectionParams = z.infer<typeof TransformFeatureSelectionParamsSchema>;
export type TransformLeakageDetectParams = z.infer<typeof TransformLeakageDetectParamsSchema>;
export type TransformQualityProfileParams = z.infer<typeof TransformQualityProfileParamsSchema>;
export type TransformDriftCompareParams = z.infer<typeof TransformDriftCompareParamsSchema>;
export type TransformDeterminismProfileParams = z.infer<typeof TransformDeterminismProfileParamsSchema>;
export type TransformFitStateRegistryParams = z.infer<typeof TransformFitStateRegistryParamsSchema>;
export type TransformPiiGuardParams = z.infer<typeof TransformPiiGuardParamsSchema>;
export type TransformInferenceParityParams = z.infer<typeof TransformInferenceParityParamsSchema>;
export type  TransformSqlParams = z.infer<typeof   TransformSqlParamsSchema>;
export type TransformSplitParams = z.infer<typeof TransformSplitParamsSchema>;
export type TransformQualityGateParams = z.infer<typeof TransformQualityGateParamsSchema>;
export type TransformMlContractParams = z.infer<typeof TransformMlContractParamsSchema>;
export type TransformJsonToTableParams = z.infer<typeof TransformJsonToTableParamsSchema>;
export type TransformTextToTableParams = z.infer<typeof TransformTextToTableParamsSchema>;
export type TransformTableToJsonParams = z.infer<typeof TransformTableToJsonParamsSchema>;


// ---- common params (shared across all ops) ----
const TransformCacheSchema = z
  .object({
    enabled: z.boolean().default(false),
    key: z.string().optional()
  })
  .strip();

const TransformCommonSchema = z
  .object({
    enabled: z.boolean().default(true),
    notes: z.string().optional().default(""),
    cache: TransformCacheSchema.optional().default({ enabled: false }),
    debug: NodeDebugParamsSchema.optional()
  })
  .strip();

// ---- node-level params schema (discriminated union) ----
export const TransformParamsSchema = z.discriminatedUnion("op", [
  TransformCommonSchema.extend({
    op: z.literal("filter"),
    filter: TransformFilterParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("json_filter"),
    json_filter: TransformJsonFilterParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("select"),
    select: TransformSelectParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("rename"),
    rename: TransformRenameParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("derive"),
    derive: TransformDeriveParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("aggregate"),
    aggregate: TransformAggregateParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("join"),
    join: TransformJoinParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("sort"),
    sort: TransformSortParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("limit"),
    limit: TransformLimitParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("dedupe"),
    dedupe: TransformDedupeParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("null_policy"),
    null_policy: TransformNullPolicyParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("outlier_policy"),
    outlier_policy: TransformOutlierPolicyParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("text_clean"),
    text_clean: TransformTextCleanParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("nlp_normalize"),
    nlp_normalize: TransformNlpNormalizeParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("tokenize_chunk"),
    tokenize_chunk: TransformTokenizeChunkParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("dataset_split"),
    dataset_split: TransformDatasetSplitParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("class_imbalance"),
    class_imbalance: TransformClassImbalanceParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("categorical_encode"),
    categorical_encode: TransformCategoricalEncodeParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("numeric_scale"),
    numeric_scale: TransformNumericScaleParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("embedding"),
    embedding: TransformEmbeddingParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("feature_selection"),
    feature_selection: TransformFeatureSelectionParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("leakage_detect"),
    leakage_detect: TransformLeakageDetectParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("quality_profile"),
    quality_profile: TransformQualityProfileParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("drift_compare"),
    drift_compare: TransformDriftCompareParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("determinism_profile"),
    determinism_profile: TransformDeterminismProfileParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("fit_state_registry"),
    fit_state_registry: TransformFitStateRegistryParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("pii_guard"),
    pii_guard: TransformPiiGuardParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("inference_parity"),
    inference_parity: TransformInferenceParityParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("split"),
    split: TransformSplitParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("quality_gate"),
    quality_gate: TransformQualityGateParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("ml_contract"),
    ml_contract: TransformMlContractParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("sql"),
    sql: TransformSqlParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("json_to_table"),
    json_to_table: TransformJsonToTableParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("text_to_table"),
    text_to_table: TransformTextToTableParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("table_to_json"),
    table_to_json: TransformTableToJsonParamsSchema
  }).strip()
]);

export type TransformParams = z.infer<typeof TransformParamsSchema>;
export type TransformKind = z.infer<typeof TransformKindSchema>;
