from __future__ import annotations

import io
import json
import hashlib
import re
import unicodedata
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - exercised in lightweight test envs
    duckdb = None
import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_string_dtype,
)

logger = logging.getLogger(__name__)

OP_KEYS = {
    "filter": "filter",
    "select": "select",
    "rename": "rename",
    "derive": "derive",
    "aggregate": "aggregate",
    "join": "join",
    "sort": "sort",
    "limit": "limit",
    "dedupe": "dedupe",
    "null_policy": "null_policy",
    "outlier_policy": "outlier_policy",
    "text_clean": "text_clean",
    "nlp_normalize": "nlp_normalize",
    "tokenize_chunk": "tokenize_chunk",
    "dataset_split": "dataset_split",
    "class_imbalance": "class_imbalance",
    "categorical_encode": "categorical_encode",
    "numeric_scale": "numeric_scale",
    "embedding": "embedding",
    "feature_selection": "feature_selection",
    "leakage_detect": "leakage_detect",
    "quality_profile": "quality_profile",
    "drift_compare": "drift_compare",
    "determinism_profile": "determinism_profile",
    "fit_state_registry": "fit_state_registry",
    "pii_guard": "pii_guard",
    "inference_parity": "inference_parity",
    "split": "split",
    "quality_gate": "quality_gate",
    "ml_contract": "ml_contract",
    "sql": "sql",
    "json_filter": "json_filter",
    "json_to_table": "json_to_table",
    "text_to_table": "text_to_table",
    "table_to_json": "table_to_json",
}

JOIN_HOWS = {"inner", "left", "right", "full"}
AGG_OPS = {
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
}
AGG_OPS_NEEDS_COLUMN = {
    "count",
    "count_distinct",
    "min",
    "max",
    "sum",
    "mean",
    "avg_length",
    "min_length",
    "max_length",
}
SELECT_MODES = {"include", "exclude"}
SELECT_KEEP_ORDER = {"input", "custom"}

# ---- helpers ----

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def quote_ident(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'

def canonical_json(obj: Any) -> str:
    # stable serialization for hashing / caching
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _stable_unique_strings(values: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for raw in values:
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out

def normalize_transform_params(params: Dict[str, Any], default_op: Optional[str] = None) -> Dict[str, Any]:
    p = dict(params)

    op = p.get("op") or default_op
    if op is None:
        inferred: List[str] = []
        for candidate_op, payload_key in OP_KEYS.items():
            if payload_key in p:
                inferred.append(candidate_op)
        inferred = sorted(set(inferred))
        if len(inferred) == 1:
            op = inferred[0]

    if op == "python":
        raise ValueError('Transform op "python" has been removed. Use Tool node provider="python".')

    if op not in OP_KEYS:
        raise ValueError(f"Transform params missing/invalid op: {op}")

    p["op"] = op

    # Legacy shape compatibility for LIMIT:
    # { op: "limit", n: 100 } -> { op: "limit", limit: { n: 100 } }
    if op == "limit" and "limit" not in p and "n" in p:
        p["limit"] = {"n": int(p.get("n") or 0)}

    p["enabled"] = bool(p.get("enabled", True))
    p.pop("notes", None)

    # normalize cache
    cache = p.get("cache") or {"enabled": False}
    if not cache.get("enabled", False):
        p["cache"] = {"enabled": False}
    else:
        # keep key only if present; drop UI junk
        p["cache"] = {"enabled": True, **({"key": cache["key"]} if cache.get("key") else {})}

    # keep only the active op payload
    keep_key = OP_KEYS[op]
    for k in ("filter","select","rename","derive","aggregate","join","sort","limit","dedupe","null_policy","outlier_policy","text_clean","nlp_normalize","tokenize_chunk","dataset_split","class_imbalance","categorical_encode","numeric_scale","embedding","feature_selection","leakage_detect","quality_profile","drift_compare","determinism_profile","fit_state_registry","pii_guard","inference_parity","split","quality_gate","ml_contract","sql","json_filter","json_to_table","text_to_table","table_to_json","code"):
        if k != keep_key:
            p.pop(k, None)

    if op == "filter":
        raw = p.get("filter") if isinstance(p.get("filter"), dict) else {}
        expr = str(raw.get("expr") or "")
        mode_raw = str(raw.get("mode") or "").strip().lower()
        if mode_raw not in {"rules", "sql"}:
            mode = "sql" if expr.strip() else "rules"
        else:
            mode = mode_raw
        rules_raw = raw.get("rules")
        if isinstance(rules_raw, dict):
            rules = rules_raw
        else:
            rules = {"kind": "group", "op": "all", "conditions": []}
        p["filter"] = {
            "mode": mode,
            "expr": expr,
            "rules": rules,
        }

    if op == "json_filter":
        raw = p.get("json_filter") if isinstance(p.get("json_filter"), dict) else {}
        mode_raw = str(raw.get("mode") or "").strip().lower()
        mode = mode_raw if mode_raw in {"rules"} else "rules"
        root_raw = raw.get("rules")
        root = root_raw if isinstance(root_raw, dict) else {"kind": "group", "op": "all", "conditions": []}
        route_reject = bool(raw.get("route_reject", True))
        include_reject_meta = bool(raw.get("include_reject_meta", True))
        p["json_filter"] = {
            "mode": mode,
            "rules": root,
            "route_reject": route_reject,
            "include_reject_meta": include_reject_meta,
        }

    if op == "derive":
        raw = p.get("derive") if isinstance(p.get("derive"), dict) else {}
        mode_raw = str(raw.get("mode") or "").strip().lower()
        columns_raw = raw.get("columns")
        columns: List[Dict[str, Any]] = []
        has_sql_expr = False
        if isinstance(columns_raw, list):
            for item in columns_raw:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                expr = str(item.get("expr") or "").strip()
                if not name or not expr:
                    continue
                has_sql_expr = True
                columns.append({"name": name, "expr": expr})
        if mode_raw not in {"rules", "sql"}:
            mode = "sql" if has_sql_expr else "rules"
        else:
            mode = mode_raw
        rules_raw = raw.get("rules")
        rules: List[Dict[str, Any]] = []
        if isinstance(rules_raw, list):
            for item in rules_raw:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                formula = item.get("formula")
                if not name or not isinstance(formula, dict):
                    continue
                rules.append({"name": name, "formula": formula})
        p["derive"] = {
            "mode": mode,
            "columns": columns,
            "rules": rules,
        }

    if op == "select":
        raw = p.get("select") if isinstance(p.get("select"), dict) else {}
        mode = str(raw.get("mode") or "include").strip().lower()
        if mode not in SELECT_MODES:
            mode = "include"
        keep_order = str(raw.get("keepOrder") or ("input" if mode == "exclude" else "custom")).strip().lower()
        if keep_order not in SELECT_KEEP_ORDER:
            keep_order = "input" if mode == "exclude" else "custom"
        strict = bool(raw.get("strict", True))
        cols_raw = raw.get("columns")
        cols: List[str] = []
        if isinstance(cols_raw, list):
            for item in cols_raw:
                col = str(item or "").strip()
                if not col:
                    continue
                cols.append(col)
        p["select"] = {
            "mode": mode,
            "columns": cols,
            "keepOrder": keep_order,
            "strict": strict,
        }

    if op == "dedupe":
        raw = p.get("dedupe") if isinstance(p.get("dedupe"), dict) else {}
        by_raw = raw.get("by")
        by: List[str] = []
        if isinstance(by_raw, list):
            seen: set[str] = set()
            for item in by_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                by.append(col)
        all_columns = bool(raw.get("allColumns", len(by) == 0))
        if all_columns:
            by = []
        keep = str(raw.get("keep") or "first")
        stable_order_column = raw.get("stableOrderColumn")
        emit_dropped_count = bool(raw.get("emitDroppedCount", False))
        dedupe_payload: Dict[str, Any] = {"allColumns": all_columns, "by": by, "keep": keep}
        if isinstance(stable_order_column, str) and str(stable_order_column).strip():
            dedupe_payload["stableOrderColumn"] = str(stable_order_column).strip()
        if emit_dropped_count:
            dedupe_payload["emitDroppedCount"] = True
        p["dedupe"] = dedupe_payload

    if op == "null_policy":
        raw = p.get("null_policy") if isinstance(p.get("null_policy"), dict) else {}
        mode = str(raw.get("mode") or "report").strip().lower()
        if mode not in {"report", "drop_rows", "fill_constant", "fill_stat"}:
            mode = "report"
        columns_raw = raw.get("columns")
        columns: List[str] = []
        if isinstance(columns_raw, list):
            seen: set[str] = set()
            for item in columns_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                columns.append(col)
        stat = str(raw.get("stat") or "mean").strip().lower()
        if stat not in {"mean", "median", "mode"}:
            stat = "mean"
        rules_raw = raw.get("rules")
        rules: List[Dict[str, Any]] = []
        if isinstance(rules_raw, list):
            for rule in rules_raw:
                if not isinstance(rule, dict):
                    continue
                column = str(rule.get("column") or "").strip()
                if not column:
                    continue
                rule_mode = str(rule.get("mode") or mode).strip().lower()
                if rule_mode not in {"report", "drop_rows", "fill_constant", "fill_stat"}:
                    rule_mode = mode
                rule_stat = str(rule.get("stat") or stat).strip().lower()
                if rule_stat not in {"mean", "median", "mode"}:
                    rule_stat = stat
                out_rule: Dict[str, Any] = {
                    "column": column,
                    "mode": rule_mode,
                    "stat": rule_stat,
                }
                if "fillValue" in rule:
                    out_rule["fillValue"] = rule.get("fillValue")
                rules.append(out_rule)
        p["null_policy"] = {
            "mode": mode,
            "columns": columns,
            "fillValue": raw.get("fillValue"),
            "stat": stat,
            "rules": rules,
        }

    if op == "outlier_policy":
        raw = p.get("outlier_policy") if isinstance(p.get("outlier_policy"), dict) else {}
        mode = str(raw.get("mode") or "clip").strip().lower()
        if mode not in {"clip", "winsorize", "drop"}:
            mode = "clip"
        method = str(raw.get("method") or "iqr").strip().lower()
        if method not in {"iqr", "zscore", "quantile"}:
            method = "iqr"
        columns_raw = raw.get("columns")
        columns: List[str] = []
        if isinstance(columns_raw, list):
            seen: set[str] = set()
            for item in columns_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                columns.append(col)
        iqr_multiplier = float(raw.get("iqrMultiplier") or 1.5)
        zscore_threshold = float(raw.get("zscoreThreshold") or 3.0)
        lower_quantile = float(raw.get("lowerQuantile") or 0.01)
        upper_quantile = float(raw.get("upperQuantile") or 0.99)
        if iqr_multiplier <= 0:
            iqr_multiplier = 1.5
        if zscore_threshold <= 0:
            zscore_threshold = 3.0
        lower_quantile = min(0.99, max(0.0, lower_quantile))
        upper_quantile = min(1.0, max(lower_quantile + 1e-6, upper_quantile))
        p["outlier_policy"] = {
            "mode": mode,
            "method": method,
            "columns": columns,
            "iqrMultiplier": iqr_multiplier,
            "zscoreThreshold": zscore_threshold,
            "lowerQuantile": lower_quantile,
            "upperQuantile": upper_quantile,
        }

    if op == "text_clean":
        raw = p.get("text_clean") if isinstance(p.get("text_clean"), dict) else {}
        cols_raw = raw.get("columns")
        columns: List[str] = []
        if isinstance(cols_raw, list):
            seen: set[str] = set()
            for item in cols_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                columns.append(col)
        unicode_normalize = str(raw.get("unicodeNormalize") or "nfkc").strip().lower()
        if unicode_normalize not in {"none", "nfc", "nfkc"}:
            unicode_normalize = "nfkc"
        p["text_clean"] = {
            "columns": columns,
            "lowercase": bool(raw.get("lowercase", True)),
            "unicodeNormalize": unicode_normalize,
            "removePunctuation": bool(raw.get("removePunctuation", False)),
            "removeUrls": bool(raw.get("removeUrls", True)),
            "removeEmails": bool(raw.get("removeEmails", True)),
            "removeEmoji": bool(raw.get("removeEmoji", False)),
            "normalizeWhitespace": bool(raw.get("normalizeWhitespace", True)),
        }

    if op == "nlp_normalize":
        raw = p.get("nlp_normalize") if isinstance(p.get("nlp_normalize"), dict) else {}
        cols_raw = raw.get("columns")
        columns: List[str] = []
        if isinstance(cols_raw, list):
            seen: set[str] = set()
            for item in cols_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                columns.append(col)
        p["nlp_normalize"] = {
            "columns": columns,
            "language": str(raw.get("language") or "en").strip().lower() or "en",
            "removeStopwords": bool(raw.get("removeStopwords", True)),
            "stemmer": str(raw.get("stemmer") or "none").strip().lower() or "none",
            "lemmatizer": str(raw.get("lemmatizer") or "none").strip().lower() or "none",
            "tokenPattern": str(raw.get("tokenPattern") or r"\w+"),
        }

    if op == "tokenize_chunk":
        raw = p.get("tokenize_chunk") if isinstance(p.get("tokenize_chunk"), dict) else {}
        cols_raw = raw.get("columns")
        columns: List[str] = []
        if isinstance(cols_raw, list):
            seen: set[str] = set()
            for item in cols_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                columns.append(col)
        tokenizer = str(raw.get("tokenizer") or "whitespace").strip().lower()
        if tokenizer not in {"whitespace", "regex"}:
            tokenizer = "whitespace"
        token_pattern = str(raw.get("tokenPattern") or r"\w+")
        max_tokens = int(raw.get("maxTokens") or 256)
        overlap = int(raw.get("overlap") or 32)
        if max_tokens < 1:
            max_tokens = 1
        if overlap < 0:
            overlap = 0
        if overlap >= max_tokens:
            overlap = max_tokens - 1
        p["tokenize_chunk"] = {
            "columns": columns,
            "tokenizer": tokenizer,
            "tokenPattern": token_pattern,
            "maxTokens": max_tokens,
            "overlap": overlap,
            "sentenceAware": bool(raw.get("sentenceAware", True)),
            "outColumn": str(raw.get("outColumn") or "chunk").strip() or "chunk",
        }

    if op == "dataset_split":
        raw = p.get("dataset_split") if isinstance(p.get("dataset_split"), dict) else {}
        strategy = str(raw.get("strategy") or "random").strip().lower()
        if strategy not in {"random", "stratified", "group", "time"}:
            strategy = "random"
        train_ratio = float(raw.get("trainRatio") or 0.8)
        val_ratio = float(raw.get("valRatio") or 0.1)
        test_ratio = float(raw.get("testRatio") or 0.1)
        if train_ratio < 0:
            train_ratio = 0.8
        if val_ratio < 0:
            val_ratio = 0.1
        if test_ratio < 0:
            test_ratio = 0.1
        ratio_sum = train_ratio + val_ratio + test_ratio
        if ratio_sum <= 0:
            train_ratio, val_ratio, test_ratio = 0.8, 0.1, 0.1
            ratio_sum = 1.0
        train_ratio /= ratio_sum
        val_ratio /= ratio_sum
        test_ratio = max(0.0, 1.0 - train_ratio - val_ratio)
        p["dataset_split"] = {
            "strategy": strategy,
            "trainRatio": train_ratio,
            "valRatio": val_ratio,
            "testRatio": test_ratio,
            "seed": int(raw.get("seed") or 42),
            "shuffle": bool(raw.get("shuffle", True)),
            "stratifyColumn": str(raw.get("stratifyColumn") or "").strip(),
            "groupColumn": str(raw.get("groupColumn") or "").strip(),
            "timeColumn": str(raw.get("timeColumn") or "").strip(),
            "leakageGuard": bool(raw.get("leakageGuard", True)),
        }

    if op == "class_imbalance":
        raw = p.get("class_imbalance") if isinstance(p.get("class_imbalance"), dict) else {}
        strategy = str(raw.get("strategy") or "report").strip().lower()
        if strategy not in {"report", "undersample", "oversample", "class_weight"}:
            strategy = "report"
        target_ratio = float(raw.get("targetRatio") or 1.0)
        target_ratio = max(0.0, min(1.0, target_ratio))
        p["class_imbalance"] = {
            "strategy": strategy,
            "labelColumn": str(raw.get("labelColumn") or "label").strip() or "label",
            "targetRatio": target_ratio,
            "seed": int(raw.get("seed") or 42),
        }

    if op == "categorical_encode":
        raw = p.get("categorical_encode") if isinstance(p.get("categorical_encode"), dict) else {}
        cols_raw = raw.get("columns")
        columns: List[str] = []
        if isinstance(cols_raw, list):
            seen: set[str] = set()
            for item in cols_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                columns.append(col)
        encoding = str(raw.get("encoding") or "one_hot").strip().lower()
        if encoding not in {"one_hot", "ordinal", "frequency"}:
            encoding = "one_hot"
        unknown_policy = str(raw.get("unknownPolicy") or "ignore").strip().lower()
        if unknown_policy not in {"ignore", "error", "impute"}:
            unknown_policy = "ignore"
        rare_threshold = float(raw.get("rareThreshold") or 0.0)
        rare_threshold = max(0.0, min(1.0, rare_threshold))
        p["categorical_encode"] = {
            "columns": columns,
            "encoding": encoding,
            "unknownPolicy": unknown_policy,
            "rareThreshold": rare_threshold,
            "dropFirst": bool(raw.get("dropFirst", False)),
        }

    if op == "numeric_scale":
        raw = p.get("numeric_scale") if isinstance(p.get("numeric_scale"), dict) else {}
        cols_raw = raw.get("columns")
        columns: List[str] = []
        if isinstance(cols_raw, list):
            seen: set[str] = set()
            for item in cols_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                columns.append(col)
        method = str(raw.get("method") or "standard").strip().lower()
        if method not in {"standard", "minmax", "robust"}:
            method = "standard"
        clip = bool(raw.get("clip", False))
        p["numeric_scale"] = {
            "columns": columns,
            "method": method,
            "withCenter": bool(raw.get("withCenter", True)),
            "withScale": bool(raw.get("withScale", True)),
            "clip": clip,
            "clipMin": raw.get("clipMin"),
            "clipMax": raw.get("clipMax"),
        }

    if op == "embedding":
        raw = p.get("embedding") if isinstance(p.get("embedding"), dict) else {}
        cols_raw = raw.get("columns")
        columns: List[str] = []
        if isinstance(cols_raw, list):
            seen: set[str] = set()
            for item in cols_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                columns.append(col)
        provider = str(raw.get("provider") or "local_hash").strip().lower()
        if provider not in {"local_hash", "openai", "ollama"}:
            provider = "local_hash"
        dimensions = int(raw.get("dimensions") or 16)
        if dimensions < 1:
            dimensions = 1
        if dimensions > 4096:
            dimensions = 4096
        p["embedding"] = {
            "columns": columns,
            "provider": provider,
            "model": str(raw.get("model") or "text-embedding-3-small"),
            "dimensions": dimensions,
            "batchSize": int(raw.get("batchSize") or 64),
            "cacheEmbeddings": bool(raw.get("cacheEmbeddings", True)),
            "outputColumn": str(raw.get("outputColumn") or "embedding").strip() or "embedding",
        }

    if op == "feature_selection":
        raw = p.get("feature_selection") if isinstance(p.get("feature_selection"), dict) else {}
        method = str(raw.get("method") or "variance").strip().lower()
        if method not in {"variance", "mutual_info", "model_importance", "manual"}:
            method = "variance"
        cols_raw = raw.get("columns")
        columns: List[str] = []
        if isinstance(cols_raw, list):
            seen: set[str] = set()
            for item in cols_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                columns.append(col)
        selected_raw = raw.get("selectedColumns")
        selected_columns: List[str] = []
        if isinstance(selected_raw, list):
            seen_sel: set[str] = set()
            for item in selected_raw:
                col = str(item or "").strip()
                if not col or col in seen_sel:
                    continue
                seen_sel.add(col)
                selected_columns.append(col)
        p["feature_selection"] = {
            "method": method,
            "columns": columns,
            "topK": max(1, int(raw.get("topK") or 50)),
            "varianceThreshold": max(0.0, float(raw.get("varianceThreshold") or 0.0)),
            "targetColumn": str(raw.get("targetColumn") or "label").strip() or "label",
            "selectedColumns": selected_columns,
        }

    if op == "leakage_detect":
        raw = p.get("leakage_detect") if isinstance(p.get("leakage_detect"), dict) else {}
        keys_raw = raw.get("keyColumns")
        key_columns: List[str] = []
        if isinstance(keys_raw, list):
            seen: set[str] = set()
            for item in keys_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                key_columns.append(col)
        p["leakage_detect"] = {
            "splitColumn": str(raw.get("splitColumn") or "split").strip() or "split",
            "keyColumns": key_columns,
            "labelColumn": str(raw.get("labelColumn") or "").strip(),
            "maxAllowedOverlap": max(0.0, min(1.0, float(raw.get("maxAllowedOverlap") or 0.0))),
        }

    if op == "quality_profile":
        raw = p.get("quality_profile") if isinstance(p.get("quality_profile"), dict) else {}
        cols_raw = raw.get("columns")
        columns: List[str] = []
        if isinstance(cols_raw, list):
            columns = [str(c).strip() for c in cols_raw if str(c).strip()]
        p["quality_profile"] = {
            "columns": _stable_unique_strings(columns),
            "includeHistograms": bool(raw.get("includeHistograms", True)),
            "includeSamples": bool(raw.get("includeSamples", True)),
        }

    if op == "drift_compare":
        raw = p.get("drift_compare") if isinstance(p.get("drift_compare"), dict) else {}
        cols_raw = raw.get("compareColumns")
        columns: List[str] = []
        if isinstance(cols_raw, list):
            columns = [str(c).strip() for c in cols_raw if str(c).strip()]
        metric = str(raw.get("metric") or "psi").strip().lower()
        if metric not in {"psi", "jsd", "ks"}:
            metric = "psi"
        p["drift_compare"] = {
            "baselineRef": str(raw.get("baselineRef") or "").strip(),
            "compareColumns": _stable_unique_strings(columns),
            "metric": metric,
            "threshold": max(0.0, float(raw.get("threshold") or 0.2)),
            "failOnDrift": bool(raw.get("failOnDrift", False)),
        }

    if op == "determinism_profile":
        raw = p.get("determinism_profile") if isinstance(p.get("determinism_profile"), dict) else {}
        p["determinism_profile"] = {
            "strict": bool(raw.get("strict", True)),
            "seed": int(raw.get("seed") or 42),
            "stableSort": bool(raw.get("stableSort", True)),
            "stableCoercion": bool(raw.get("stableCoercion", True)),
        }

    if op == "fit_state_registry":
        raw = p.get("fit_state_registry") if isinstance(p.get("fit_state_registry"), dict) else {}
        mode = str(raw.get("mode") or "fit").strip().lower()
        if mode not in {"fit", "apply"}:
            mode = "fit"
        cols_raw = raw.get("includeColumns")
        columns = [str(c).strip() for c in cols_raw if str(c).strip()] if isinstance(cols_raw, list) else []
        p["fit_state_registry"] = {
            "mode": mode,
            "stateKey": str(raw.get("stateKey") or "default").strip() or "default",
            "includeColumns": _stable_unique_strings(columns),
        }

    if op == "pii_guard":
        raw = p.get("pii_guard") if isinstance(p.get("pii_guard"), dict) else {}
        action = str(raw.get("action") or "report").strip().lower()
        if action not in {"report", "mask", "drop_rows"}:
            action = "report"
        cols_raw = raw.get("columns")
        columns = [str(c).strip() for c in cols_raw if str(c).strip()] if isinstance(cols_raw, list) else []
        p["pii_guard"] = {
            "columns": _stable_unique_strings(columns),
            "action": action,
            "failOnDetect": bool(raw.get("failOnDetect", False)),
        }

    if op == "inference_parity":
        raw = p.get("inference_parity") if isinstance(p.get("inference_parity"), dict) else {}
        p["inference_parity"] = {
            "trainSignature": str(raw.get("trainSignature") or "").strip(),
            "inferenceSignature": str(raw.get("inferenceSignature") or "").strip(),
            "failOnMismatch": bool(raw.get("failOnMismatch", True)),
        }

    if op == "join":
        raw = p.get("join") if isinstance(p.get("join"), dict) else {}
        clauses_raw = raw.get("clauses")
        clauses: List[Dict[str, Any]] = []
        if isinstance(clauses_raw, list):
            for item in clauses_raw:
                if not isinstance(item, dict):
                    continue
                left_node_id = str(item.get("leftNodeId") or "").strip()
                left_col = str(item.get("leftCol") or "").strip()
                right_node_id = str(item.get("rightNodeId") or "").strip()
                right_col = str(item.get("rightCol") or "").strip()
                how = str(item.get("how") or "inner").strip().lower()
                if how not in JOIN_HOWS:
                    how = "inner"
                if not left_node_id or not left_col or not right_node_id or not right_col:
                    continue
                clauses.append({
                    "leftNodeId": left_node_id,
                    "leftCol": left_col,
                    "rightNodeId": right_node_id,
                    "rightCol": right_col,
                    "how": how,
                })
        p["join"] = {"clauses": clauses}

    if op == "aggregate":
        raw = p.get("aggregate") if isinstance(p.get("aggregate"), dict) else {}
        group_by_raw = raw.get("groupBy")
        group_by: List[str] = []
        if isinstance(group_by_raw, list):
            seen_group: set[str] = set()
            for item in group_by_raw:
                col = str(item or "").strip()
                if not col or col in seen_group:
                    continue
                seen_group.add(col)
                group_by.append(col)

        metrics_raw = raw.get("metrics")
        metrics: List[Dict[str, Any]] = []
        if isinstance(metrics_raw, list):
            seen_names: set[str] = set()
            for item in metrics_raw:
                if not isinstance(item, dict):
                    continue
                # legacy compatibility: {as, expr}
                if "as" in item or "expr" in item:
                    legacy_name = str(item.get("as") or "").strip()
                    legacy_expr = str(item.get("expr") or "").strip()
                    if not legacy_name or not legacy_expr:
                        continue
                    fn_match = re.match(r"^\s*([a-z_]+)\((.*)\)\s*$", legacy_expr, flags=re.IGNORECASE)
                    op_name = "count_rows"
                    column_name: Optional[str] = None
                    if fn_match:
                        fn = str(fn_match.group(1) or "").lower().strip()
                        arg = str(fn_match.group(2) or "").strip().strip('"')
                        if fn == "count" and arg == "*":
                            op_name = "count_rows"
                        elif fn == "avg":
                            length_match = re.match(r"^length\(([^)]+)\)$", arg, flags=re.IGNORECASE)
                            if length_match:
                                op_name = "avg_length"
                                column_name = str(length_match.group(1) or "").strip().strip('"')
                            else:
                                op_name = "mean"
                                column_name = arg
                        elif fn == "count_distinct":
                            op_name = "count_distinct"
                            column_name = arg
                        elif fn in AGG_OPS:
                            op_name = fn
                            column_name = arg if fn in AGG_OPS_NEEDS_COLUMN else None
                    if legacy_name in seen_names:
                        continue
                    seen_names.add(legacy_name)
                    metrics.append({
                        "name": legacy_name,
                        "op": op_name,
                        "column": column_name,
                    })
                    continue

                name = str(item.get("name") or "").strip()
                op_name = str(item.get("op") or "").strip()
                column_name = str(item.get("column") or "").strip()
                if not name or name in seen_names:
                    continue
                if op_name not in AGG_OPS:
                    continue
                seen_names.add(name)
                metrics.append({
                    "name": name,
                    "op": op_name,
                    "column": column_name if op_name in AGG_OPS_NEEDS_COLUMN else None,
                })

        if not metrics:
            metrics = [{"name": "row_count", "op": "count_rows", "column": None}]

        p["aggregate"] = {
            "groupBy": group_by,
            "metrics": metrics,
        }

    if op == "quality_gate":
        raw = p.get("quality_gate") if isinstance(p.get("quality_gate"), dict) else {}
        checks_raw = raw.get("checks")
        checks: List[Dict[str, Any]] = []
        if isinstance(checks_raw, list):
            for item in checks_raw:
                if not isinstance(item, dict):
                    continue
                kind = str(item.get("kind") or "").strip().lower()
                severity = str(item.get("severity") or "fail").strip().lower()
                severity = "warn" if severity == "warn" else "fail"
                if kind == "null_pct":
                    column = str(item.get("column") or "").strip()
                    if not column:
                        continue
                    max_null_pct = float(item.get("maxNullPct") or 0.0)
                    max_null_pct = min(1.0, max(0.0, max_null_pct))
                    checks.append({
                        "kind": "null_pct",
                        "column": column,
                        "maxNullPct": max_null_pct,
                        "severity": severity,
                    })
                    continue
                if kind == "range":
                    column = str(item.get("column") or "").strip()
                    if not column:
                        continue
                    has_min = item.get("min") is not None and str(item.get("min")).strip() != ""
                    has_max = item.get("max") is not None and str(item.get("max")).strip() != ""
                    if not has_min and not has_max:
                        continue
                    check: Dict[str, Any] = {"kind": "range", "column": column, "severity": severity}
                    if has_min:
                        check["min"] = float(item.get("min"))
                    if has_max:
                        check["max"] = float(item.get("max"))
                    check["inclusiveMin"] = bool(item.get("inclusiveMin", True))
                    check["inclusiveMax"] = bool(item.get("inclusiveMax", True))
                    max_out_of_range_pct = float(item.get("maxOutOfRangePct") or 0.0)
                    check["maxOutOfRangePct"] = min(1.0, max(0.0, max_out_of_range_pct))
                    checks.append(check)
                    continue
                if kind == "uniqueness":
                    column = str(item.get("column") or "").strip()
                    if not column:
                        continue
                    min_unique_ratio = float(item.get("minUniqueRatio") or 0.0)
                    checks.append({
                        "kind": "uniqueness",
                        "column": column,
                        "minUniqueRatio": min(1.0, max(0.0, min_unique_ratio)),
                        "severity": severity,
                    })
                    continue
                if kind == "class_balance":
                    column = str(item.get("column") or "").strip()
                    if not column:
                        continue
                    min_minority_ratio = float(item.get("minMinorityRatio") or 0.0)
                    max_dominant_ratio = float(item.get("maxDominantRatio") or 1.0)
                    checks.append({
                        "kind": "class_balance",
                        "column": column,
                        "minMinorityRatio": min(1.0, max(0.0, min_minority_ratio)),
                        "maxDominantRatio": min(1.0, max(0.0, max_dominant_ratio)),
                        "severity": severity,
                    })
                    continue
                if kind == "leakage":
                    feature_column = str(item.get("featureColumn") or "").strip()
                    target_column = str(item.get("targetColumn") or "").strip()
                    if not feature_column or not target_column:
                        continue
                    max_abs_correlation = float(item.get("maxAbsCorrelation") or 1.0)
                    checks.append({
                        "kind": "leakage",
                        "featureColumn": feature_column,
                        "targetColumn": target_column,
                        "maxAbsCorrelation": min(1.0, max(0.0, max_abs_correlation)),
                        "severity": severity,
                    })
                    continue
        p["quality_gate"] = {
            "checks": checks,
            "stopOnFail": bool(raw.get("stopOnFail", True)),
        }

    if op == "ml_contract":
        raw = p.get("ml_contract") if isinstance(p.get("ml_contract"), dict) else {}
        task_type = str(raw.get("taskType") or "other").strip().lower()
        allowed_task_types = {
            "classification",
            "regression",
            "ranking",
            "generation",
            "embedding",
            "pretraining",
            "finetuning",
            "other",
        }
        if task_type not in allowed_task_types:
            task_type = "other"
        label_column = str(raw.get("labelColumn") or "label").strip() or "label"
        features_raw = raw.get("featureColumns")
        feature_columns: List[str] = []
        if isinstance(features_raw, list):
            seen: set[str] = set()
            for item in features_raw:
                col = str(item or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                feature_columns.append(col)
        if not feature_columns:
            feature_columns = ["text"]
        id_column = str(raw.get("idColumn") or "").strip()
        timestamp_column = str(raw.get("timestampColumn") or "").strip()
        p["ml_contract"] = {
            "taskType": task_type,
            "labelColumn": label_column,
            "featureColumns": feature_columns,
            "idColumn": id_column,
            "timestampColumn": timestamp_column,
            "allowExtraFeatures": bool(raw.get("allowExtraFeatures", True)),
            "requireNonNullLabel": bool(raw.get("requireNonNullLabel", True)),
        }

    if op == "json_to_table":
        raw = p.get("json_to_table") if isinstance(p.get("json_to_table"), dict) else {}
        orient = str(raw.get("orient") or "records").strip().lower()
        if orient not in {"records", "object"}:
            orient = "records"
        rows_key = str(raw.get("rowsKey") or "rows").strip() or "rows"
        p["json_to_table"] = {
            "orient": orient,
            "rowsKey": rows_key,
        }

    if op == "text_to_table":
        raw = p.get("text_to_table") if isinstance(p.get("text_to_table"), dict) else {}
        mode = str(raw.get("mode") or "lines").strip().lower()
        if mode not in {"lines", "csv", "tsv"}:
            mode = "lines"
        column = str(raw.get("column") or "text").strip() or "text"
        delimiter = str(raw.get("delimiter") or ",")
        p["text_to_table"] = {
            "mode": mode,
            "column": column,
            "delimiter": delimiter,
            "hasHeader": bool(raw.get("hasHeader", True)),
        }

    if op == "table_to_json":
        raw = p.get("table_to_json") if isinstance(p.get("table_to_json"), dict) else {}
        orient = str(raw.get("orient") or "records").strip().lower()
        if orient not in {"records", "split"}:
            orient = "records"
        p["table_to_json"] = {
            "orient": orient,
            "pretty": bool(raw.get("pretty", False)),
        }

    return p

def inputs_fingerprint(inputs: List[Tuple[str, str]]) -> List[Dict[str, str]]:
    """
    inputs = [(input_handle, upstream_artifact_id), ...]
    Return stable sorted list.
    """
    return [{"inputHandle": input_handle, "artifactId": aid} for input_handle, aid in sorted(inputs, key=lambda x: x[0])]

# ---- data contracts ----

@dataclass(frozen=True)
class TransformResult:
    payload_bytes: bytes
    mime_type: str
    meta: Dict[str, Any]
    additional_outputs: Dict[str, "TransformAdditionalOutput"] = field(default_factory=dict)


@dataclass(frozen=True)
class TransformAdditionalOutput:
    payload_bytes: bytes
    mime_type: str
    meta: Dict[str, Any]

# ---- table IO ----

def normalize_mime_type(raw: str) -> str:
    return (raw or "").split(";", 1)[0].strip().lower()


def _load_table_from_json_text(s: str) -> pd.DataFrame:
    # JSON array/object first, then JSONL fallback.
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        if isinstance(obj, dict):
            if "rows" in obj and isinstance(obj["rows"], list):
                return pd.DataFrame(obj["rows"])
            return pd.DataFrame([obj])
    except Exception:
        pass

    rows = [json.loads(line) for line in s.splitlines() if line.strip()]
    return pd.DataFrame(rows)


def _load_table_from_plain_text(b: bytes) -> pd.DataFrame:
    s = b.decode("utf-8", errors="replace")
    # Deterministic text-table bridge: one non-empty line per row.
    lines = [ln for ln in s.splitlines() if ln.strip()]
    if not lines:
        return pd.DataFrame({"text": []})
    return pd.DataFrame({"text": lines})


def _json_rows_to_df(value: Any, *, rows_key: str) -> pd.DataFrame:
    if isinstance(value, list):
        if all(isinstance(item, dict) for item in value):
            return pd.DataFrame(value)
        return pd.DataFrame({"value": list(value)})
    if isinstance(value, dict):
        if rows_key in value and isinstance(value.get(rows_key), list):
            rows_value = value.get(rows_key) or []
            if all(isinstance(item, dict) for item in rows_value):
                return pd.DataFrame(rows_value)
            return pd.DataFrame({"value": list(rows_value)})
        return pd.DataFrame([value])
    if value is None:
        return pd.DataFrame()
    return pd.DataFrame({"value": [value]})


def load_table_from_json_bytes(
    b: bytes,
    *,
    orient: str = "records",
    rows_key: str = "rows",
) -> pd.DataFrame:
    text = b.decode("utf-8", errors="replace")
    if not text.strip():
        return pd.DataFrame()
    obj: Any
    try:
        obj = json.loads(text)
    except Exception:
        # Fallback to JSONL for newline-delimited records.
        jsonl_rows = [json.loads(line) for line in text.splitlines() if line.strip()]
        obj = jsonl_rows

    mode = str(orient or "records").strip().lower()
    if mode == "object" and isinstance(obj, dict):
        if rows_key in obj:
            return _json_rows_to_df(obj.get(rows_key), rows_key=rows_key)
        return _json_rows_to_df(obj, rows_key=rows_key)
    return _json_rows_to_df(obj, rows_key=rows_key)


def load_table_from_text_bytes(
    b: bytes,
    *,
    mode: str = "lines",
    column: str = "text",
    delimiter: str = ",",
    has_header: bool = True,
) -> pd.DataFrame:
    text = b.decode("utf-8", errors="replace")
    normalized_mode = str(mode or "lines").strip().lower()
    out_column = str(column or "text").strip() or "text"

    if normalized_mode == "lines":
        lines = [ln for ln in text.splitlines() if ln.strip()]
        return pd.DataFrame({out_column: lines})

    if normalized_mode in {"csv", "tsv"}:
        sep = "\t" if normalized_mode == "tsv" else (delimiter or ",")
        header = 0 if has_header else None
        df = pd.read_csv(io.StringIO(text), sep=sep, header=header)
        if not has_header:
            df.columns = [
                out_column if idx == 0 else f"{out_column}_{idx}"
                for idx in range(len(df.columns))
            ]
        return df

    return pd.DataFrame({out_column: [text] if text else []})


def load_table_from_artifact_bytes(mime_type: str, b: bytes) -> pd.DataFrame:
    mt = normalize_mime_type(mime_type)

    # CSV / delimited text
    if mt in ("text/csv", "application/csv", "text/tab-separated-values"):
        sep = "\t" if mt == "text/tab-separated-values" else ","
        return pd.read_csv(io.BytesIO(b), sep=sep)
    if mt in ("text/plain", "application/plain"):
        return _load_table_from_plain_text(b)

    # JSON / JSONL
    if mt in ("application/json", "application/x-ndjson", "application/jsonl"):
        s = b.decode("utf-8", errors="replace")
        return _load_table_from_json_text(s)

    # Parquet
    if mt in ("application/vnd.apache.parquet", "application/x-parquet"):
        return pd.read_parquet(io.BytesIO(b))

    # Excel
    if mt in (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
    ):
        return pd.read_excel(io.BytesIO(b))

    raise ValueError(
        "Unsupported input mime_type for Transform table operations: "
        f"{mime_type!r}. Supported: text/csv, text/tab-separated-values, "
        "application/json, application/x-ndjson, application/jsonl, "
        "application/vnd.apache.parquet, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet."
    )

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    # Determinism: stable column order as currently in df; stable line endings; no index.
    out = io.StringIO()
    df.to_csv(out, index=False, lineterminator="\n")
    return out.getvalue().encode("utf-8")


def df_to_json_bytes(df: pd.DataFrame, *, orient: str = "records", pretty: bool = False) -> bytes:
    if orient == "split":
        payload = {
            "columns": [str(c) for c in list(df.columns)],
            "index": [int(i) for i in range(len(df))],
            "data": df.values.tolist(),
        }
    else:
        payload = df.to_dict(orient="records")
    if pretty:
        return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _dtype_to_transform_type(series: pd.Series) -> str:
    dtype = series.dtype
    if is_bool_dtype(dtype):
        return "bool"
    if is_integer_dtype(dtype):
        return "int"
    if is_float_dtype(dtype):
        return "float"
    if is_datetime64_any_dtype(dtype):
        return "datetime"
    if is_string_dtype(dtype):
        return "string"
    return "unknown"


def _null_ratio_by_column(df: pd.DataFrame) -> Dict[str, float]:
    row_count = int(len(df))
    if row_count <= 0:
        return {str(col): 0.0 for col in list(df.columns)}
    out: Dict[str, float] = {}
    for col in list(df.columns):
        col_name = str(col)
        null_count = int(df[col].isna().sum())
        out[col_name] = float(null_count) / float(row_count)
    return out


def _column_types(df: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for col in list(df.columns):
        col_name = str(col)
        try:
            out[col_name] = _dtype_to_transform_type(df[col])
        except Exception:
            out[col_name] = "unknown"
    return out


def _table_schema_snapshot(df: pd.DataFrame) -> Dict[str, Any]:
    columns = [str(c) for c in list(df.columns)]
    return {
        "type": "table",
        "columns": [{"name": col, "type": _column_types(df).get(col, "unknown")} for col in columns],
        "rowCount": int(len(df)),
    }


def _determinism_profile(op: str, params: Dict[str, Any]) -> Dict[str, Any]:
    strict_mode = True
    stable_sort = True
    stable_type_coercion = True
    seed = 0
    if op == "sort":
        stable_sort = len((params.get("sort") or {}).get("by") or []) > 0
    if op == "sql":
        stable_sort = False
    if op == "text_to_table":
        stable_type_coercion = str((params.get("text_to_table") or {}).get("mode") or "lines") == "lines"
    return {
        "profile": "strict_repro_v1",
        "strict": strict_mode,
        "seed": seed,
        "stableSort": stable_sort,
        "stableTypeCoercion": stable_type_coercion,
    }


def _cost_plan(op: str, params: Dict[str, Any], in_rows: int, in_cols: int) -> Dict[str, Any]:
    score = float(in_rows) * max(float(in_cols), 1.0)
    recommendations: List[str] = []
    heavy_ops = {"join", "aggregate", "sort", "sql", "split"}
    if op in heavy_ops:
        score *= 2.0
    if op == "join":
        clause_count = len((params.get("join") or {}).get("clauses") or [])
        score *= max(float(clause_count), 1.0)
        recommendations.append("Consider pre-filtering or pre-limiting both join inputs.")
    if op in {"sort", "aggregate", "sql", "join"} and in_rows > 50000:
        recommendations.append("Add a LIMIT earlier or filter rows before this transform.")
    if op == "split":
        recommendations.append("Cap maxParts to control row explosion.")
    if score >= 2_000_000:
        tier = "high"
    elif score >= 250_000:
        tier = "medium"
    else:
        tier = "low"
    if not recommendations and op in {"filter", "select", "rename", "derive"}:
        recommendations.append("Safe early-stage op: keep before heavy transforms.")
    return {
        "estimatedTier": tier,
        "estimatedScore": round(score, 2),
        "recommendations": recommendations,
    }


def _row_level_diagnostics(input_df: pd.DataFrame, output_df: pd.DataFrame) -> Dict[str, Any]:
    in_cols = [str(c) for c in list(input_df.columns)]
    out_cols = [str(c) for c in list(output_df.columns)]
    common = [c for c in in_cols if c in set(out_cols)]
    preview_in = input_df.head(3).where(input_df.head(3).notna(), None).to_dict(orient="records")
    preview_out = output_df.head(3).where(output_df.head(3).notna(), None).to_dict(orient="records")
    per_column_changed_ratio: Dict[str, float] = {}
    if common and len(input_df) > 0 and len(output_df) > 0:
        aligned_len = min(len(input_df), len(output_df))
        left = input_df.iloc[:aligned_len]
        right = output_df.iloc[:aligned_len]
        for col in common:
            try:
                neq = (left[col].astype("string") != right[col].astype("string")).fillna(False)
                per_column_changed_ratio[col] = float(neq.mean())
            except Exception:
                per_column_changed_ratio[col] = 0.0
    return {
        "sampleIn": preview_in,
        "sampleOut": preview_out,
        "columnChangeRatio": per_column_changed_ratio,
    }


def _build_execution_metadata(
    *,
    op: str,
    params: Dict[str, Any],
    input_df: pd.DataFrame,
    output_df: pd.DataFrame,
    elapsed_ms: float,
) -> Dict[str, Any]:
    in_rows = int(len(input_df))
    out_rows = int(len(output_df))
    in_cols = [str(c) for c in list(input_df.columns)]
    out_cols = [str(c) for c in list(output_df.columns)]
    in_null = _null_ratio_by_column(input_df)
    out_null = _null_ratio_by_column(output_df)
    in_types = _column_types(input_df)
    out_types = _column_types(output_df)

    common_cols = sorted(set(in_cols).intersection(out_cols))
    null_drift = {
        col: float(out_null.get(col, 0.0)) - float(in_null.get(col, 0.0))
        for col in common_cols
    }
    type_drift = [
        {"column": col, "from": in_types.get(col, "unknown"), "to": out_types.get(col, "unknown")}
        for col in common_cols
        if in_types.get(col, "unknown") != out_types.get(col, "unknown")
    ]
    schema_before = _table_schema_snapshot(input_df)
    schema_after = _table_schema_snapshot(output_df)
    planner = _cost_plan(op, params, in_rows, len(in_cols))
    determinism = _determinism_profile(op, params)
    row_diag = _row_level_diagnostics(input_df, output_df)

    return {
        "op": str(op or ""),
        "input": {
            "rows": in_rows,
            "columns": in_cols,
            "nullRatioByColumn": in_null,
            "typesByColumn": in_types,
        },
        "output": {
            "rows": out_rows,
            "columns": out_cols,
            "nullRatioByColumn": out_null,
            "typesByColumn": out_types,
        },
        "drift": {
            "rowDelta": out_rows - in_rows,
            "rowRatio": (float(out_rows) / float(in_rows)) if in_rows > 0 else None,
            "addedColumns": [c for c in out_cols if c not in set(in_cols)],
            "removedColumns": [c for c in in_cols if c not in set(out_cols)],
            "nullRatioDeltaByColumn": null_drift,
            "typeChanges": type_drift,
        },
        "cost": {
            "elapsedMs": round(max(0.0, float(elapsed_ms)), 3),
            "rowsIn": in_rows,
            "rowsOut": out_rows,
        },
        "schemaChecks": {
            "mandatory": True,
            "before": {"ok": True, "schema": schema_before},
            "after": {"ok": True, "schema": schema_after},
        },
        "determinism": determinism,
        "planner": planner,
        "timeline": [
            {"phase": "schema_check_before", "rows": in_rows, "columns": len(in_cols)},
            {"phase": "execute_op", "op": str(op or ""), "elapsedMs": round(max(0.0, float(elapsed_ms)), 3)},
            {"phase": "schema_check_after", "rows": out_rows, "columns": len(out_cols)},
        ],
        "rowDiagnostics": row_diag,
    }


def _execute_split_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    source_col = str(spec.get("sourceColumn") or "text")
    out_col = str(spec.get("outColumn") or "part")
    mode = str(spec.get("mode") or "sentences")
    line_break = str(spec.get("lineBreak") or "any").strip().lower()
    pattern = str(spec.get("pattern") or "")
    delimiter = str(spec.get("delimiter") or "")
    flags_raw = str(spec.get("flags") or "")
    trim = bool(spec.get("trim", True))
    drop_empty = bool(spec.get("dropEmpty", True))
    emit_index = bool(spec.get("emitIndex", True))
    emit_source_row = bool(spec.get("emitSourceRow", True))
    max_parts = int(spec.get("maxParts") or 5000)
    max_parts = max(1, min(100000, max_parts))
    max_chars = 2_000_000

    flag_value = 0
    if "i" in flags_raw:
        flag_value |= re.IGNORECASE
    if "m" in flags_raw:
        flag_value |= re.MULTILINE
    if "s" in flags_raw:
        flag_value |= re.DOTALL

    if mode == "regex":
        if not pattern.strip():
            raise ValueError("split.pattern is required when mode='regex'")
        splitter = re.compile(pattern, flags=flag_value)
    else:
        splitter = None

    if mode == "delimiter":
        if delimiter == "":
            raise ValueError("split.delimiter is required when mode='delimiter'")
        delimiter = delimiter.replace("\\r", "\r").replace("\\n", "\n").replace("\\t", "\t")

    if line_break not in {"any", "lf", "crlf", "cr"}:
        line_break = "any"

    def _split_text(text: str) -> List[str]:
        if mode == "lines":
            if line_break == "lf":
                return text.split("\n")
            if line_break == "crlf":
                return text.split("\r\n")
            if line_break == "cr":
                return text.split("\r")
            return re.split(r"\r\n|\n|\r", text)
        if mode == "sentences":
            normalized = text.replace("\r\n", "\n").replace("\r", "\n")
            normalized = re.sub(r"\s+", " ", normalized).strip()
            if not normalized:
                return []
            return re.split(r"(?<=[.!?])\s+", normalized)
        if mode == "regex":
            assert splitter is not None
            return splitter.split(text)
        if mode == "delimiter":
            return text.split(delimiter)
        return [text]

    rows_out: List[Dict[str, Any]] = []
    for src_idx, row in enumerate(primary_df.to_dict(orient="records")):
        value = row.get(source_col, "")
        text = "" if value is None else str(value)
        if len(text) > max_chars:
            raise ValueError(
                f"split source value exceeds max chars ({len(text)} > {max_chars}) for row={src_idx}"
            )
        parts = _split_text(text)
        emitted = 0
        for idx, part in enumerate(parts):
            token = part.strip() if trim else part
            if drop_empty and token == "":
                continue
            if emitted >= max_parts:
                logger.warning(
                    "Split capped: emitted=%s parts (maxParts=%s) for row=%s",
                    emitted,
                    max_parts,
                    src_idx,
                )
                break
            out_row: Dict[str, Any] = {out_col: token}
            if emit_index:
                out_row["index"] = idx
            if emit_source_row:
                out_row["source_row"] = src_idx
            rows_out.append(out_row)
            emitted += 1
    return pd.DataFrame(rows_out)


def _execute_dedupe_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    """
    Deduplicate is a logical TABLE_V1 transform.
    - by=[] means dedupe on the entire row.
    - keep='first' keeps the first row by stable row order (__rowid).
    """
    all_columns = bool(spec.get("allColumns", False))
    by_raw = spec.get("by")
    by: List[str] = []
    if isinstance(by_raw, list):
        seen: set[str] = set()
        for item in by_raw:
            col = str(item or "").strip()
            if not col or col in seen:
                continue
            seen.add(col)
            by.append(col)
    if all_columns:  # legacy compatibility
        by = []
    keep = str(spec.get("keep") or "first")
    if keep != "first":
        raise ValueError("dedupe.keep must be 'first'")
    if (not all_columns) and (len(by) == 0):
        raise ValueError("dedupe.by must include at least one column when allColumns=false")

    cols = [str(c) for c in list(primary_df.columns)]
    missing = [c for c in by if c not in cols]
    if missing:
        raise ValueError(f"dedupe.by columns missing from input: {', '.join(missing)}")

    working = primary_df.reset_index(drop=True).copy()
    working["__rowid"] = range(len(working))
    working = working.sort_values("__rowid", kind="stable")

    if by:
        deduped = working.drop_duplicates(subset=by, keep="first")
    else:
        deduped = working.drop_duplicates(subset=cols, keep="first") if cols else working.head(1)

    deduped = deduped.sort_values("__rowid", kind="stable").drop(columns=["__rowid"], errors="ignore")
    return deduped.reset_index(drop=True)


def _execute_aggregate_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    group_by_raw = spec.get("groupBy")
    metrics_raw = spec.get("metrics")
    group_by: List[str] = []
    if isinstance(group_by_raw, list):
        seen_group: set[str] = set()
        for item in group_by_raw:
            col = str(item or "").strip()
            if not col or col in seen_group:
                continue
            seen_group.add(col)
            group_by.append(col)

    metrics: List[Dict[str, Any]] = []
    if isinstance(metrics_raw, list):
        seen_names: set[str] = set()
        for item in metrics_raw:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            op = str(item.get("op") or "").strip()
            column = str(item.get("column") or "").strip()
            if not name or name in seen_names:
                continue
            if op not in AGG_OPS:
                continue
            if op in AGG_OPS_NEEDS_COLUMN and not column:
                continue
            seen_names.add(name)
            metrics.append({"name": name, "op": op, "column": column or None})
    if not metrics:
        metrics = [{"name": "row_count", "op": "count_rows", "column": None}]

    cols = [str(c) for c in list(primary_df.columns)]
    missing_group = [c for c in group_by if c not in cols]
    missing_metric = [m["column"] for m in metrics if m.get("column") and m["column"] not in cols]
    missing = sorted(set([*missing_group, *missing_metric]))
    if missing:
        raise ValueError(f"aggregate columns missing from input: {', '.join(missing)}")

    if len(group_by) == 0:
        out_row: Dict[str, Any] = {}
        for metric in metrics:
            name = metric["name"]
            op = metric["op"]
            col = metric.get("column")
            if op == "count_rows":
                out_row[name] = int(len(primary_df))
            elif op == "count":
                out_row[name] = int(primary_df[col].count())
            elif op == "count_distinct":
                out_row[name] = int(primary_df[col].nunique(dropna=True))
            elif op == "min":
                out_row[name] = primary_df[col].min()
            elif op == "max":
                out_row[name] = primary_df[col].max()
            elif op == "sum":
                out_row[name] = primary_df[col].sum()
            elif op == "mean":
                out_row[name] = primary_df[col].mean()
            elif op in {"avg_length", "min_length", "max_length"}:
                lengths = primary_df[col].astype("string").str.len()
                if op == "avg_length":
                    out_row[name] = lengths.mean()
                elif op == "min_length":
                    out_row[name] = lengths.min()
                else:
                    out_row[name] = lengths.max()
        return pd.DataFrame([out_row], columns=[m["name"] for m in metrics])

    grouped = primary_df.groupby(group_by, dropna=False, sort=True)
    out = grouped.size().reset_index(name="__group_size")
    out = out.drop(columns=["__group_size"], errors="ignore")
    for metric in metrics:
        name = metric["name"]
        op = metric["op"]
        col = metric.get("column")
        if op == "count_rows":
            series = grouped.size()
        elif op == "count":
            series = grouped[col].count()
        elif op == "count_distinct":
            series = grouped[col].nunique(dropna=True)
        elif op == "min":
            series = grouped[col].min()
        elif op == "max":
            series = grouped[col].max()
        elif op == "sum":
            series = grouped[col].sum()
        elif op == "mean":
            series = grouped[col].mean()
        elif op in {"avg_length", "min_length", "max_length"}:
            lengths = primary_df[col].astype("string").str.len()
            if op == "avg_length":
                series = lengths.groupby([primary_df[g] for g in group_by], dropna=False).mean()
            elif op == "min_length":
                series = lengths.groupby([primary_df[g] for g in group_by], dropna=False).min()
            else:
                series = lengths.groupby([primary_df[g] for g in group_by], dropna=False).max()
        else:
            continue
        out = out.merge(series.rename(name).reset_index(), on=group_by, how="left")

    out = out.sort_values(by=group_by, kind="stable", na_position="last").reset_index(drop=True)
    ordered_cols = group_by + [m["name"] for m in metrics]
    return out.reindex(columns=ordered_cols)


def _quality_gate_report(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Dict[str, Any]:
    checks = spec.get("checks") if isinstance(spec.get("checks"), list) else []
    columns = [str(c) for c in list(primary_df.columns)]
    col_set = set(columns)
    fail_violations: List[Dict[str, Any]] = []
    warn_violations: List[Dict[str, Any]] = []

    def add_violation(*, severity: str, payload: Dict[str, Any]) -> None:
        if severity == "warn":
            warn_violations.append(payload)
        else:
            fail_violations.append(payload)

    for idx, raw in enumerate(checks):
        if not isinstance(raw, dict):
            continue
        kind = str(raw.get("kind") or "").strip().lower()
        severity = "warn" if str(raw.get("severity") or "fail").strip().lower() == "warn" else "fail"
        if kind == "null_pct":
            column = str(raw.get("column") or "").strip()
            if column not in col_set:
                add_violation(severity=severity, payload={
                    "index": idx, "kind": kind, "severity": severity, "column": column,
                    "reason": "missing_column",
                })
                continue
            max_null_pct = float(raw.get("maxNullPct") or 0.0)
            series = primary_df[column]
            total = int(len(series))
            null_count = int(series.isna().sum())
            null_pct = (float(null_count) / float(total)) if total > 0 else 0.0
            if null_pct > max_null_pct:
                add_violation(severity=severity, payload={
                    "index": idx,
                    "kind": kind,
                    "severity": severity,
                    "column": column,
                    "observedNullPct": null_pct,
                    "thresholdMaxNullPct": max_null_pct,
                })
            continue

        if kind == "range":
            column = str(raw.get("column") or "").strip()
            if column not in col_set:
                add_violation(severity=severity, payload={
                    "index": idx, "kind": kind, "severity": severity, "column": column,
                    "reason": "missing_column",
                })
                continue
            has_min = "min" in raw and raw.get("min") is not None and str(raw.get("min")).strip() != ""
            has_max = "max" in raw and raw.get("max") is not None and str(raw.get("max")).strip() != ""
            if not has_min and not has_max:
                continue
            min_value = float(raw.get("min")) if has_min else None
            max_value = float(raw.get("max")) if has_max else None
            inclusive_min = bool(raw.get("inclusiveMin", True))
            inclusive_max = bool(raw.get("inclusiveMax", True))
            max_out_of_range_pct = float(raw.get("maxOutOfRangePct") or 0.0)
            numeric = pd.to_numeric(primary_df[column], errors="coerce")
            non_null = numeric.dropna()
            denom = int(len(non_null))
            if denom == 0:
                continue
            outside_mask = pd.Series(False, index=non_null.index)
            if min_value is not None:
                if inclusive_min:
                    outside_mask = outside_mask | (non_null < min_value)
                else:
                    outside_mask = outside_mask | (non_null <= min_value)
            if max_value is not None:
                if inclusive_max:
                    outside_mask = outside_mask | (non_null > max_value)
                else:
                    outside_mask = outside_mask | (non_null >= max_value)
            outside = int(outside_mask.sum())
            outside_pct = float(outside) / float(denom)
            if outside_pct > max_out_of_range_pct:
                add_violation(severity=severity, payload={
                    "index": idx,
                    "kind": kind,
                    "severity": severity,
                    "column": column,
                    "observedOutOfRangePct": outside_pct,
                    "thresholdMaxOutOfRangePct": max_out_of_range_pct,
                    "min": min_value,
                    "max": max_value,
                })
            continue

        if kind == "uniqueness":
            column = str(raw.get("column") or "").strip()
            if column not in col_set:
                add_violation(severity=severity, payload={
                    "index": idx, "kind": kind, "severity": severity, "column": column,
                    "reason": "missing_column",
                })
                continue
            min_unique_ratio = float(raw.get("minUniqueRatio") or 0.0)
            series = primary_df[column].dropna()
            denom = int(len(series))
            unique_ratio = (float(series.nunique(dropna=True)) / float(denom)) if denom > 0 else 0.0
            if unique_ratio < min_unique_ratio:
                add_violation(severity=severity, payload={
                    "index": idx,
                    "kind": kind,
                    "severity": severity,
                    "column": column,
                    "observedUniqueRatio": unique_ratio,
                    "thresholdMinUniqueRatio": min_unique_ratio,
                })
            continue

        if kind == "class_balance":
            column = str(raw.get("column") or "").strip()
            if column not in col_set:
                add_violation(severity=severity, payload={
                    "index": idx, "kind": kind, "severity": severity, "column": column,
                    "reason": "missing_column",
                })
                continue
            min_minority_ratio = float(raw.get("minMinorityRatio") or 0.0)
            max_dominant_ratio = float(raw.get("maxDominantRatio") or 1.0)
            series = primary_df[column].dropna()
            if len(series) == 0:
                continue
            ratios = series.value_counts(normalize=True, dropna=True)
            dominant_ratio = float(ratios.max()) if len(ratios) > 0 else 0.0
            minority_ratio = float(ratios.min()) if len(ratios) > 0 else 0.0
            if dominant_ratio > max_dominant_ratio or minority_ratio < min_minority_ratio:
                add_violation(severity=severity, payload={
                    "index": idx,
                    "kind": kind,
                    "severity": severity,
                    "column": column,
                    "observedDominantRatio": dominant_ratio,
                    "observedMinorityRatio": minority_ratio,
                    "thresholdMaxDominantRatio": max_dominant_ratio,
                    "thresholdMinMinorityRatio": min_minority_ratio,
                })
            continue

        if kind == "leakage":
            feature_column = str(raw.get("featureColumn") or "").strip()
            target_column = str(raw.get("targetColumn") or "").strip()
            if feature_column not in col_set or target_column not in col_set:
                add_violation(severity=severity, payload={
                    "index": idx,
                    "kind": kind,
                    "severity": severity,
                    "featureColumn": feature_column,
                    "targetColumn": target_column,
                    "reason": "missing_column",
                })
                continue
            max_abs_corr = float(raw.get("maxAbsCorrelation") or 1.0)
            left = pd.to_numeric(primary_df[feature_column], errors="coerce")
            right = pd.to_numeric(primary_df[target_column], errors="coerce")
            if left.isna().all():
                left_codes, _ = pd.factorize(primary_df[feature_column], sort=True)
                left = pd.Series(left_codes, index=primary_df.index, dtype="float64").where(left_codes >= 0)
            if right.isna().all():
                right_codes, _ = pd.factorize(primary_df[target_column], sort=True)
                right = pd.Series(right_codes, index=primary_df.index, dtype="float64").where(right_codes >= 0)
            pair = pd.DataFrame({"left": left, "right": right}).dropna()
            if len(pair) < 2:
                continue
            corr = float(pair["left"].corr(pair["right"]))
            if pd.isna(corr):
                continue
            abs_corr = abs(corr)
            if abs_corr > max_abs_corr:
                add_violation(severity=severity, payload={
                    "index": idx,
                    "kind": kind,
                    "severity": severity,
                    "featureColumn": feature_column,
                    "targetColumn": target_column,
                    "observedAbsCorrelation": abs_corr,
                    "thresholdMaxAbsCorrelation": max_abs_corr,
                })
            continue

    return {
        "checksEvaluated": int(len(checks)),
        "failViolations": fail_violations,
        "warnViolations": warn_violations,
        "failed": bool(fail_violations),
    }


def _quality_gate_failure_message(report: Dict[str, Any]) -> str:
    violations = report.get("failViolations") if isinstance(report.get("failViolations"), list) else []
    if not violations:
        return "quality_gate failed"
    first = violations[0] if isinstance(violations[0], dict) else {}
    kind = str(first.get("kind") or "unknown")
    if "column" in first:
        return f"quality_gate failed: {kind} on column {first.get('column')}"
    if "featureColumn" in first and "targetColumn" in first:
        return (
            f"quality_gate failed: {kind} on "
            f"{first.get('featureColumn')}->{first.get('targetColumn')}"
        )
    return f"quality_gate failed: {kind}"


def _execute_quality_gate_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    report = _quality_gate_report(primary_df, spec)
    if bool(report.get("failed")):
        raise ValueError(_quality_gate_failure_message(report))
    return primary_df


def _execute_null_policy_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    mode = str(spec.get("mode") or "report").strip().lower()
    if mode not in {"report", "drop_rows", "fill_constant", "fill_stat"}:
        mode = "report"
    columns_raw = spec.get("columns")
    selected_columns = (
        [str(c).strip() for c in columns_raw if str(c).strip()]
        if isinstance(columns_raw, list)
        else []
    )
    existing_cols = [str(c) for c in list(primary_df.columns)]
    target_cols = [c for c in selected_columns if c in set(existing_cols)] if selected_columns else existing_cols
    rules_raw = spec.get("rules")
    rules = [r for r in rules_raw if isinstance(r, dict)] if isinstance(rules_raw, list) else []
    rule_by_column = {
        str(r.get("column") or "").strip(): r
        for r in rules
        if str(r.get("column") or "").strip()
    }
    working = primary_df.copy()
    before_null_counts = {
        col: int(working[col].isna().sum()) if col in working.columns else 0
        for col in target_cols
    }
    dropped_rows = 0
    filled_by_column: Dict[str, int] = {}
    fill_value_global = spec.get("fillValue")
    stat_global = str(spec.get("stat") or "mean").strip().lower()
    if stat_global not in {"mean", "median", "mode"}:
        stat_global = "mean"

    if mode == "drop_rows":
        if target_cols:
            before_rows = int(len(working))
            working = working.dropna(subset=target_cols).reset_index(drop=True)
            dropped_rows = before_rows - int(len(working))
    elif mode in {"fill_constant", "fill_stat"}:
        for col in target_cols:
            rule = rule_by_column.get(col) or {}
            rule_mode = str(rule.get("mode") or mode).strip().lower()
            if rule_mode == "report":
                continue
            if rule_mode == "drop_rows":
                before_rows = int(len(working))
                working = working.dropna(subset=[col]).reset_index(drop=True)
                dropped_rows += before_rows - int(len(working))
                continue
            if rule_mode == "fill_constant":
                fill_value = rule.get("fillValue", fill_value_global)
                null_mask = working[col].isna()
                count = int(null_mask.sum())
                if count > 0:
                    working.loc[null_mask, col] = fill_value
                filled_by_column[col] = filled_by_column.get(col, 0) + count
                continue
            if rule_mode == "fill_stat":
                stat = str(rule.get("stat") or stat_global).strip().lower()
                if stat not in {"mean", "median", "mode"}:
                    stat = stat_global
                series = working[col]
                non_null = series.dropna()
                if len(non_null) == 0:
                    fill_value = fill_value_global
                elif stat == "mean":
                    fill_value = float(non_null.mean())
                elif stat == "median":
                    fill_value = float(non_null.median())
                else:
                    modes = non_null.mode()
                    fill_value = modes.iloc[0] if len(modes) > 0 else fill_value_global
                null_mask = series.isna()
                count = int(null_mask.sum())
                if count > 0:
                    working.loc[null_mask, col] = fill_value
                filled_by_column[col] = filled_by_column.get(col, 0) + count

    after_null_counts = {
        col: int(working[col].isna().sum()) if col in working.columns else 0
        for col in target_cols
    }
    report = {
        "mode": mode,
        "targetColumns": target_cols,
        "beforeNullCountByColumn": before_null_counts,
        "afterNullCountByColumn": after_null_counts,
        "filledCountByColumn": filled_by_column,
        "droppedRows": int(dropped_rows),
    }
    return working, report


def _execute_outlier_policy_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    mode = str(spec.get("mode") or "clip").strip().lower()
    if mode not in {"clip", "winsorize", "drop"}:
        mode = "clip"
    method = str(spec.get("method") or "iqr").strip().lower()
    if method not in {"iqr", "zscore", "quantile"}:
        method = "iqr"
    selected_raw = spec.get("columns")
    selected = (
        [str(c).strip() for c in selected_raw if str(c).strip()]
        if isinstance(selected_raw, list)
        else []
    )
    numeric_cols = [
        str(c)
        for c in list(primary_df.columns)
        if pd.api.types.is_numeric_dtype(primary_df[c])
    ]
    target_cols = [c for c in selected if c in set(numeric_cols)] if selected else numeric_cols
    working = primary_df.copy()
    per_column: Dict[str, Any] = {}
    drop_mask = pd.Series(False, index=working.index)
    iqr_multiplier = float(spec.get("iqrMultiplier") or 1.5)
    zscore_threshold = float(spec.get("zscoreThreshold") or 3.0)
    lower_q = float(spec.get("lowerQuantile") or 0.01)
    upper_q = float(spec.get("upperQuantile") or 0.99)

    for col in target_cols:
        series = pd.to_numeric(working[col], errors="coerce")
        non_null = series.dropna()
        if len(non_null) == 0:
            continue
        lower = float(non_null.min())
        upper = float(non_null.max())
        if method == "iqr":
            q1 = float(non_null.quantile(0.25))
            q3 = float(non_null.quantile(0.75))
            iqr = q3 - q1
            lower = q1 - (iqr_multiplier * iqr)
            upper = q3 + (iqr_multiplier * iqr)
        elif method == "zscore":
            mean = float(non_null.mean())
            std = float(non_null.std(ddof=0))
            if std > 0:
                lower = mean - (zscore_threshold * std)
                upper = mean + (zscore_threshold * std)
        else:
            lower = float(non_null.quantile(lower_q))
            upper = float(non_null.quantile(upper_q))
        outlier_mask = (series < lower) | (series > upper)
        outlier_count = int(outlier_mask.fillna(False).sum())
        if mode in {"clip", "winsorize"} and outlier_count > 0:
            working[col] = series.clip(lower=lower, upper=upper)
        elif mode == "drop":
            drop_mask = drop_mask | outlier_mask.fillna(False)
        per_column[col] = {
            "lower": lower,
            "upper": upper,
            "outlierCount": outlier_count,
        }
    dropped_rows = int(drop_mask.sum()) if mode == "drop" else 0
    if mode == "drop" and dropped_rows > 0:
        working = working.loc[~drop_mask].reset_index(drop=True)
    report = {
        "mode": mode,
        "method": method,
        "targetColumns": target_cols,
        "perColumn": per_column,
        "droppedRows": dropped_rows,
    }
    return working, report


def _execute_text_clean_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cols_raw = spec.get("columns")
    selected = (
        [str(c).strip() for c in cols_raw if str(c).strip()]
        if isinstance(cols_raw, list)
        else []
    )
    existing_cols = [str(c) for c in list(primary_df.columns)]
    target_cols = [c for c in selected if c in set(existing_cols)] if selected else existing_cols
    lowercase = bool(spec.get("lowercase", True))
    unicode_norm = str(spec.get("unicodeNormalize") or "nfkc").strip().lower()
    remove_punct = bool(spec.get("removePunctuation", False))
    remove_urls = bool(spec.get("removeUrls", True))
    remove_emails = bool(spec.get("removeEmails", True))
    remove_emoji = bool(spec.get("removeEmoji", False))
    normalize_ws = bool(spec.get("normalizeWhitespace", True))
    if unicode_norm not in {"none", "nfc", "nfkc"}:
        unicode_norm = "nfkc"

    url_re = re.compile(r"https?://\S+|www\.\S+", flags=re.IGNORECASE)
    email_re = re.compile(r"\b[\w.\-+]+@[\w.\-]+\.\w+\b", flags=re.IGNORECASE)
    punct_re = re.compile(r"[^\w\s]")
    emoji_re = re.compile(r"[\U00010000-\U0010ffff]", flags=re.UNICODE)
    ws_re = re.compile(r"\s+")

    def clean_text(value: Any) -> str:
        text = "" if value is None else str(value)
        if unicode_norm != "none":
            text = unicodedata.normalize(unicode_norm.upper(), text)
        if lowercase:
            text = text.lower()
        if remove_urls:
            text = url_re.sub(" ", text)
        if remove_emails:
            text = email_re.sub(" ", text)
        if remove_emoji:
            text = emoji_re.sub(" ", text)
        if remove_punct:
            text = punct_re.sub(" ", text)
        if normalize_ws:
            text = ws_re.sub(" ", text).strip()
        return text

    working = primary_df.copy()
    changed_by_column: Dict[str, int] = {}
    for col in target_cols:
        series = working[col]
        as_text = series.map(lambda v: "" if v is None else str(v))
        cleaned = series.map(clean_text)
        changed = int((as_text != cleaned).sum())
        working[col] = cleaned
        changed_by_column[col] = changed
    report = {
        "targetColumns": target_cols,
        "changedRowsByColumn": changed_by_column,
        "options": {
            "lowercase": lowercase,
            "unicodeNormalize": unicode_norm,
            "removePunctuation": remove_punct,
            "removeUrls": remove_urls,
            "removeEmails": remove_emails,
            "removeEmoji": remove_emoji,
            "normalizeWhitespace": normalize_ws,
        },
    }
    return working, report


def _simple_porter_stem(token: str) -> str:
    t = token.lower()
    for suffix in ("ingly", "edly", "ing", "ed", "ies", "sses", "s"):
        if t.endswith(suffix) and len(t) > (len(suffix) + 2):
            if suffix == "ies":
                return t[:-3] + "y"
            if suffix == "sses":
                return t[:-2]
            return t[: -len(suffix)]
    return t


def _simple_rule_lemma(token: str) -> str:
    t = token.lower()
    irregular = {"mice": "mouse", "geese": "goose", "children": "child", "men": "man", "women": "woman"}
    if t in irregular:
        return irregular[t]
    if t.endswith("ies") and len(t) > 4:
        return t[:-3] + "y"
    if t.endswith("ves") and len(t) > 4:
        return t[:-3] + "f"
    if t.endswith("s") and len(t) > 3 and not t.endswith("ss"):
        return t[:-1]
    return t


def _execute_nlp_normalize_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    language = str(spec.get("language") or "en").strip().lower()
    if language != "en":
        raise ValueError(f"nlp_normalize.language '{language}' is not supported; supported languages: en")
    pattern = str(spec.get("tokenPattern") or r"\w+")
    token_re = re.compile(pattern, flags=re.UNICODE)
    remove_stopwords = bool(spec.get("removeStopwords", True))
    stemmer = str(spec.get("stemmer") or "none").strip().lower()
    lemmatizer = str(spec.get("lemmatizer") or "none").strip().lower()
    if stemmer not in {"none", "porter"}:
        stemmer = "none"
    if lemmatizer not in {"none", "rule_based"}:
        lemmatizer = "none"

    stopwords_en = {
        "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
        "in", "is", "it", "of", "on", "or", "that", "the", "to", "was", "were", "with",
    }

    cols_raw = spec.get("columns")
    selected = (
        [str(c).strip() for c in cols_raw if str(c).strip()]
        if isinstance(cols_raw, list)
        else []
    )
    existing_cols = [str(c) for c in list(primary_df.columns)]
    target_cols = [c for c in selected if c in set(existing_cols)] if selected else existing_cols
    working = primary_df.copy()
    changed_by_column: Dict[str, int] = {}
    removed_stopwords_by_column: Dict[str, int] = {}
    token_count_by_column: Dict[str, int] = {}

    for col in target_cols:
        original = working[col].map(lambda v: "" if v is None else str(v))
        changed = 0
        removed_stopwords = 0
        token_total = 0
        normalized_values: List[str] = []
        for value in original:
            tokens = token_re.findall(str(value).lower())
            token_total += len(tokens)
            out_tokens: List[str] = []
            for tok in tokens:
                next_tok = tok
                if remove_stopwords and next_tok in stopwords_en:
                    removed_stopwords += 1
                    continue
                if stemmer == "porter":
                    next_tok = _simple_porter_stem(next_tok)
                if lemmatizer == "rule_based":
                    next_tok = _simple_rule_lemma(next_tok)
                out_tokens.append(next_tok)
            normalized = " ".join(out_tokens)
            if normalized != value:
                changed += 1
            normalized_values.append(normalized)
        working[col] = normalized_values
        changed_by_column[col] = changed
        removed_stopwords_by_column[col] = removed_stopwords
        token_count_by_column[col] = token_total

    report = {
        "language": language,
        "targetColumns": target_cols,
        "changedRowsByColumn": changed_by_column,
        "removedStopwordsByColumn": removed_stopwords_by_column,
        "inputTokenCountByColumn": token_count_by_column,
        "stemmer": stemmer,
        "lemmatizer": lemmatizer,
    }
    return working, report


def _execute_tokenize_chunk_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    tokenizer = str(spec.get("tokenizer") or "whitespace").strip().lower()
    if tokenizer not in {"whitespace", "regex"}:
        tokenizer = "whitespace"
    token_pattern = str(spec.get("tokenPattern") or r"\w+")
    max_tokens = int(spec.get("maxTokens") or 256)
    overlap = int(spec.get("overlap") or 32)
    sentence_aware = bool(spec.get("sentenceAware", True))
    out_col = str(spec.get("outColumn") or "chunk").strip() or "chunk"
    if max_tokens < 1:
        max_tokens = 1
    if overlap < 0:
        overlap = 0
    if overlap >= max_tokens:
        overlap = max_tokens - 1

    cols_raw = spec.get("columns")
    selected = (
        [str(c).strip() for c in cols_raw if str(c).strip()]
        if isinstance(cols_raw, list)
        else []
    )
    existing_cols = [str(c) for c in list(primary_df.columns)]
    target_cols = [c for c in selected if c in set(existing_cols)] if selected else existing_cols

    token_re = re.compile(token_pattern, flags=re.UNICODE)
    sentence_re = re.compile(r"(?<=[.!?])\s+")

    out_rows: List[Dict[str, Any]] = []
    dropped_token_count = 0
    total_chunks = 0
    token_histogram: Dict[str, int] = {}
    for row_idx, row in enumerate(primary_df.to_dict(orient="records")):
        for col in target_cols:
            raw_text = "" if row.get(col) is None else str(row.get(col))
            segments = sentence_re.split(raw_text) if sentence_aware else [raw_text]
            tokens: List[str] = []
            if tokenizer == "whitespace":
                for seg in segments:
                    tokens.extend([t for t in seg.split() if t])
            else:
                for seg in segments:
                    tokens.extend(token_re.findall(seg))
            if not tokens:
                continue
            start = 0
            step = max(1, max_tokens - overlap)
            chunk_index = 0
            while start < len(tokens):
                chunk_tokens = tokens[start : start + max_tokens]
                if not chunk_tokens:
                    break
                out_rows.append(
                    {
                        out_col: " ".join(chunk_tokens),
                        "source_row": row_idx,
                        "source_column": col,
                        "chunk_index": chunk_index,
                        "token_count": len(chunk_tokens),
                    }
                )
                token_histogram[str(len(chunk_tokens))] = int(token_histogram.get(str(len(chunk_tokens)), 0)) + 1
                total_chunks += 1
                chunk_index += 1
                start += step
            consumed = max_tokens + max(0, (chunk_index - 1) * step) if chunk_index > 0 else 0
            if consumed < len(tokens):
                dropped_token_count += (len(tokens) - consumed)
    out_df = pd.DataFrame(out_rows)
    report = {
        "targetColumns": target_cols,
        "tokenizer": tokenizer,
        "maxTokens": max_tokens,
        "overlap": overlap,
        "sentenceAware": sentence_aware,
        "chunkStats": {
            "numChunks": total_chunks,
            "tokenHistogram": token_histogram,
            "droppedTokens": dropped_token_count,
        },
    }
    return out_df, report


def _stable_shuffle_indices(size: int, seed: int) -> List[int]:
    if size <= 0:
        return []
    rng = pd.Series(range(size)).sample(frac=1.0, random_state=seed)
    return [int(i) for i in rng.tolist()]


def _execute_dataset_split_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    n = int(len(primary_df))
    if n == 0:
        out = primary_df.copy()
        out["split"] = pd.Series(dtype="string")
        return out, {"strategy": str(spec.get("strategy") or "random"), "counts": {"train": 0, "val": 0, "test": 0}}

    strategy = str(spec.get("strategy") or "random").strip().lower()
    train_ratio = float(spec.get("trainRatio") or 0.8)
    val_ratio = float(spec.get("valRatio") or 0.1)
    seed = int(spec.get("seed") or 42)
    shuffle = bool(spec.get("shuffle", True))
    stratify_col = str(spec.get("stratifyColumn") or "").strip()
    group_col = str(spec.get("groupColumn") or "").strip()
    time_col = str(spec.get("timeColumn") or "").strip()

    labels = pd.Series(["test"] * n, index=primary_df.index, dtype="string")

    def assign_by_index(order_idx: List[int]) -> None:
        train_n = int(round(n * train_ratio))
        val_n = int(round(n * val_ratio))
        train_n = max(0, min(n, train_n))
        val_n = max(0, min(n - train_n, val_n))
        train_ids = order_idx[:train_n]
        val_ids = order_idx[train_n : train_n + val_n]
        if train_ids:
            labels.iloc[train_ids] = "train"
        if val_ids:
            labels.iloc[val_ids] = "val"

    if strategy == "time" and time_col and time_col in set(primary_df.columns):
        ordered = primary_df.reset_index(drop=True).sort_values(by=time_col, kind="mergesort")
        assign_by_index([int(i) for i in ordered.index.tolist()])
    elif strategy == "group" and group_col and group_col in set(primary_df.columns):
        group_series = primary_df[group_col].astype("string")
        group_keys = [str(v) for v in group_series.fillna("__null_group__").tolist()]
        unique_groups = sorted(set(group_keys))
        if shuffle:
            shuffled = _stable_shuffle_indices(len(unique_groups), seed)
            unique_groups = [unique_groups[i] for i in shuffled]
        group_to_split: Dict[str, str] = {}
        g_total = len(unique_groups)
        g_train = int(round(g_total * train_ratio))
        g_val = int(round(g_total * val_ratio))
        for idx, grp in enumerate(unique_groups):
            if idx < g_train:
                group_to_split[grp] = "train"
            elif idx < g_train + g_val:
                group_to_split[grp] = "val"
            else:
                group_to_split[grp] = "test"
        labels = pd.Series([group_to_split.get(k, "test") for k in group_keys], index=primary_df.index, dtype="string")
    elif strategy == "stratified" and stratify_col and stratify_col in set(primary_df.columns):
        strat = primary_df[stratify_col].astype("string").fillna("__null__")
        labels = pd.Series(["test"] * n, index=primary_df.index, dtype="string")
        for _, idx_values in strat.groupby(strat).groups.items():
            idx_list = [int(primary_df.index.get_loc(idx)) for idx in list(idx_values)]
            if shuffle:
                idx_list = [idx_list[i] for i in _stable_shuffle_indices(len(idx_list), seed)]
            sub_n = len(idx_list)
            sub_train = int(round(sub_n * train_ratio))
            sub_val = int(round(sub_n * val_ratio))
            for i in idx_list[:sub_train]:
                labels.iloc[i] = "train"
            for i in idx_list[sub_train : sub_train + sub_val]:
                labels.iloc[i] = "val"
    else:
        order = list(range(n))
        if shuffle:
            order = _stable_shuffle_indices(n, seed)
        assign_by_index(order)

    out_df = primary_df.copy()
    out_df["split"] = labels.values
    split_counts = labels.value_counts(dropna=False).to_dict()
    report: Dict[str, Any] = {
        "strategy": strategy,
        "seed": seed,
        "counts": {
            "train": int(split_counts.get("train", 0)),
            "val": int(split_counts.get("val", 0)),
            "test": int(split_counts.get("test", 0)),
        },
    }
    if strategy == "stratified" and stratify_col in set(primary_df.columns):
        per_split: Dict[str, Dict[str, int]] = {}
        series = primary_df[stratify_col].astype("string").fillna("__null__")
        for split_name in ("train", "val", "test"):
            mask = out_df["split"] == split_name
            per_split[split_name] = {str(k): int(v) for k, v in series[mask].value_counts(dropna=False).to_dict().items()}
        report["stratifiedDistributions"] = per_split
    return out_df, report


def _execute_class_imbalance_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    strategy = str(spec.get("strategy") or "report").strip().lower()
    label_col = str(spec.get("labelColumn") or "label").strip() or "label"
    seed = int(spec.get("seed") or 42)
    target_ratio = float(spec.get("targetRatio") or 1.0)
    if label_col not in set(primary_df.columns):
        raise ValueError(f"class_imbalance.labelColumn '{label_col}' not found")
    out_df = primary_df.copy()
    y = out_df[label_col].astype("string")
    before = {str(k): int(v) for k, v in y.value_counts(dropna=False).to_dict().items()}
    class_weights: Dict[str, float] = {}
    if strategy in {"undersample", "oversample"} and len(before) > 1:
        major = max(before.values())
        minor = min(before.values())
        target = int(round(major * target_ratio))
        target = max(minor, target)
        parts: List[pd.DataFrame] = []
        for cls, count in before.items():
            cls_df = out_df[y == cls]
            if strategy == "undersample" and count > target:
                cls_df = cls_df.sample(n=target, random_state=seed)
            elif strategy == "oversample" and count < target:
                extra = cls_df.sample(n=(target - count), replace=True, random_state=seed)
                cls_df = pd.concat([cls_df, extra], ignore_index=False)
            parts.append(cls_df)
        out_df = pd.concat(parts, ignore_index=True)
    elif strategy == "class_weight":
        total = max(1, int(len(out_df)))
        n_classes = max(1, int(len(before)))
        for cls, count in before.items():
            class_weights[str(cls)] = float(total / (n_classes * max(1, int(count))))
    after = {str(k): int(v) for k, v in out_df[label_col].astype("string").value_counts(dropna=False).to_dict().items()}
    report: Dict[str, Any] = {
        "strategy": strategy,
        "labelColumn": label_col,
        "before": before,
        "after": after,
    }
    if class_weights:
        report["classWeights"] = class_weights
    return out_df, report


def _execute_categorical_encode_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    encoding = str(spec.get("encoding") or "one_hot").strip().lower()
    columns_raw = spec.get("columns")
    selected = [str(c).strip() for c in columns_raw if str(c).strip()] if isinstance(columns_raw, list) else []
    existing_cols = [str(c) for c in list(primary_df.columns)]
    target_cols = [c for c in selected if c in set(existing_cols)] if selected else [
        c for c in existing_cols if is_string_dtype(primary_df[c]) or str(primary_df[c].dtype) == "category"
    ]
    out_df = primary_df.copy()
    mapping: Dict[str, Any] = {}
    if encoding == "one_hot":
        dummies = pd.get_dummies(out_df[target_cols], prefix=target_cols, drop_first=bool(spec.get("dropFirst", False)))
        out_df = pd.concat([out_df.drop(columns=target_cols), dummies], axis=1)
        mapping["emittedColumns"] = [str(c) for c in list(dummies.columns)]
    elif encoding == "ordinal":
        for col in target_cols:
            vals = out_df[col].astype("string").fillna("__null__")
            cats = sorted(set([str(v) for v in vals.tolist()]))
            cat_to_idx = {cat: idx for idx, cat in enumerate(cats)}
            out_df[col] = vals.map(cat_to_idx).astype("int64")
            mapping[col] = cat_to_idx
    else:
        for col in target_cols:
            vals = out_df[col].astype("string").fillna("__null__")
            freqs = vals.value_counts(normalize=True, dropna=False).to_dict()
            out_df[col] = vals.map(freqs).astype("float64")
            mapping[col] = {str(k): float(v) for k, v in freqs.items()}
    return out_df, {"encoding": encoding, "columns": target_cols, "mapping": mapping}


def _execute_numeric_scale_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    method = str(spec.get("method") or "standard").strip().lower()
    columns_raw = spec.get("columns")
    selected = [str(c).strip() for c in columns_raw if str(c).strip()] if isinstance(columns_raw, list) else []
    existing_cols = [str(c) for c in list(primary_df.columns)]
    if selected:
        target_cols = [c for c in selected if c in set(existing_cols)]
    else:
        target_cols = [c for c in existing_cols if is_integer_dtype(primary_df[c]) or is_float_dtype(primary_df[c])]
    out = primary_df.copy()
    params_out: Dict[str, Dict[str, float]] = {}
    for col in target_cols:
        s = pd.to_numeric(out[col], errors="coerce")
        if method == "minmax":
            mn = float(s.min()) if pd.notna(s.min()) else 0.0
            mx = float(s.max()) if pd.notna(s.max()) else 1.0
            denom = (mx - mn) if (mx - mn) != 0 else 1.0
            out[col] = (s - mn) / denom
            params_out[col] = {"min": mn, "max": mx}
        elif method == "robust":
            q1 = float(s.quantile(0.25)) if not s.empty else 0.0
            q3 = float(s.quantile(0.75)) if not s.empty else 1.0
            med = float(s.median()) if not s.empty else 0.0
            iqr = (q3 - q1) if (q3 - q1) != 0 else 1.0
            out[col] = (s - med) / iqr
            params_out[col] = {"median": med, "iqr": iqr}
        else:
            mean = float(s.mean()) if not s.empty else 0.0
            std = float(s.std(ddof=0)) if not s.empty else 1.0
            if std == 0:
                std = 1.0
            out[col] = (s - mean) / std
            params_out[col] = {"mean": mean, "std": std}
    if bool(spec.get("clip", False)):
        clip_min = spec.get("clipMin")
        clip_max = spec.get("clipMax")
        out[target_cols] = out[target_cols].clip(lower=clip_min, upper=clip_max)
    return out, {"method": method, "columns": target_cols, "state": params_out}


def _stable_embedding_vector(text: str, dimensions: int) -> List[float]:
    dims = max(1, int(dimensions))
    digest = hashlib.sha256(text.encode("utf-8", errors="ignore")).digest()
    values: List[float] = []
    needed = dims
    seed_bytes = digest
    while needed > 0:
        for b in seed_bytes:
            values.append((float(b) / 255.0) * 2.0 - 1.0)
            needed -= 1
            if needed <= 0:
                break
        seed_bytes = hashlib.sha256(seed_bytes).digest()
    return values[:dims]


def _execute_embedding_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    provider = str(spec.get("provider") or "local_hash").strip().lower()
    model = str(spec.get("model") or "text-embedding-3-small")
    dimensions = int(spec.get("dimensions") or 16)
    out_col = str(spec.get("outputColumn") or "embedding").strip() or "embedding"
    columns_raw = spec.get("columns")
    selected = [str(c).strip() for c in columns_raw if str(c).strip()] if isinstance(columns_raw, list) else []
    existing_cols = [str(c) for c in list(primary_df.columns)]
    target_cols = [c for c in selected if c in set(existing_cols)] if selected else existing_cols[:1]
    if not target_cols:
        target_cols = []
    out = primary_df.copy()
    vectors: List[List[float]] = []
    token_estimate = 0
    for _, row in out.iterrows():
        text = " ".join([str(row.get(c) or "") for c in target_cols]).strip()
        token_estimate += len(text.split())
        vectors.append(_stable_embedding_vector(text, dimensions))
    out[out_col] = vectors
    report = {
        "provider": provider,
        "model": model,
        "dimensions": dimensions,
        "columns": target_cols,
        "tokenEstimate": token_estimate,
        "costEstimateUsd": float(token_estimate) * 0.0,
    }
    return out, report


def _execute_feature_selection_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    method = str(spec.get("method") or "variance").strip().lower()
    columns_raw = spec.get("columns")
    selected = [str(c).strip() for c in columns_raw if str(c).strip()] if isinstance(columns_raw, list) else []
    existing_cols = [str(c) for c in list(primary_df.columns)]
    candidate_cols = [c for c in selected if c in set(existing_cols)] if selected else [
        c for c in existing_cols if is_integer_dtype(primary_df[c]) or is_float_dtype(primary_df[c])
    ]
    top_k = max(1, int(spec.get("topK") or 50))
    variance_threshold = max(0.0, float(spec.get("varianceThreshold") or 0.0))
    chosen: List[str] = []
    scores: Dict[str, float] = {}
    if method == "manual":
        manual = spec.get("selectedColumns")
        chosen = [str(c).strip() for c in manual if str(c).strip() in set(existing_cols)] if isinstance(manual, list) else []
    else:
        for col in candidate_cols:
            s = pd.to_numeric(primary_df[col], errors="coerce")
            score = float(s.var(ddof=0)) if not s.empty else 0.0
            scores[col] = score
        if method == "variance":
            chosen = [c for c in candidate_cols if scores.get(c, 0.0) >= variance_threshold]
        else:
            ranked = sorted(candidate_cols, key=lambda c: scores.get(c, 0.0), reverse=True)
            chosen = ranked[:top_k]
    if not chosen:
        chosen = candidate_cols[:top_k]
    out = primary_df[chosen].copy() if chosen else primary_df.iloc[:, 0:0].copy()
    return out, {"method": method, "selectedColumns": chosen, "scores": scores}


def _execute_leakage_detect_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    split_col = str(spec.get("splitColumn") or "split").strip() or "split"
    key_cols = [str(c).strip() for c in (spec.get("keyColumns") or []) if str(c).strip()]
    label_col = str(spec.get("labelColumn") or "").strip()
    max_allowed = float(spec.get("maxAllowedOverlap") or 0.0)
    if split_col not in set(primary_df.columns):
        raise ValueError(f"leakage_detect.splitColumn '{split_col}' not found")
    if not key_cols:
        key_cols = [c for c in list(primary_df.columns) if c != split_col][:1]
    keyed = primary_df.copy()
    keyed["_leak_key"] = keyed[key_cols].astype("string").agg("|".join, axis=1)
    overlap_pairs: List[Dict[str, Any]] = []
    splits = [str(v) for v in keyed[split_col].astype("string").fillna("__null__").unique().tolist()]
    split_to_keys: Dict[str, set[str]] = {
        sp: set(keyed[keyed[split_col].astype("string") == sp]["_leak_key"].tolist()) for sp in splits
    }
    max_overlap = 0.0
    for i, a in enumerate(splits):
        for b in splits[i + 1 :]:
            inter = split_to_keys[a].intersection(split_to_keys[b])
            union = split_to_keys[a].union(split_to_keys[b]) or {""}
            ratio = float(len(inter)) / float(len(union))
            max_overlap = max(max_overlap, ratio)
            overlap_pairs.append({"left": a, "right": b, "overlapRatio": ratio, "overlapCount": len(inter)})
    report = {
        "splitColumn": split_col,
        "keyColumns": key_cols,
        "labelColumn": label_col,
        "maxAllowedOverlap": max_allowed,
        "maxObservedOverlap": max_overlap,
        "violated": bool(max_overlap > max_allowed),
        "pairs": overlap_pairs,
    }
    return primary_df.copy(), report


def _execute_quality_profile_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cols_raw = spec.get("columns")
    selected = [str(c).strip() for c in cols_raw if str(c).strip()] if isinstance(cols_raw, list) else []
    target_cols = [c for c in selected if c in set(primary_df.columns)] if selected else [str(c) for c in list(primary_df.columns)]
    profile: Dict[str, Any] = {"rowCount": int(len(primary_df)), "columns": {}}
    include_hist = bool(spec.get("includeHistograms", True))
    include_samples = bool(spec.get("includeSamples", True))
    for col in target_cols:
        s = primary_df[col]
        col_profile: Dict[str, Any] = {
            "dtype": str(s.dtype),
            "nullCount": int(s.isna().sum()),
            "nullPct": float(s.isna().mean()) if len(s) else 0.0,
            "nUnique": int(s.nunique(dropna=True)),
        }
        if is_integer_dtype(s) or is_float_dtype(s):
            col_profile["mean"] = float(pd.to_numeric(s, errors="coerce").mean())
            col_profile["std"] = float(pd.to_numeric(s, errors="coerce").std(ddof=0))
        if include_hist:
            top = s.astype("string").value_counts(dropna=False).head(10).to_dict()
            col_profile["topValues"] = {str(k): int(v) for k, v in top.items()}
        if include_samples:
            col_profile["samples"] = [None if pd.isna(v) else str(v) for v in s.head(5).tolist()]
        profile["columns"][col] = col_profile
    return primary_df.copy(), profile


def _execute_drift_compare_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    compare_raw = spec.get("compareColumns")
    compare_cols = [str(c).strip() for c in compare_raw if str(c).strip()] if isinstance(compare_raw, list) else []
    metric = str(spec.get("metric") or "psi").strip().lower()
    threshold = float(spec.get("threshold") or 0.2)
    if "split" not in set(primary_df.columns):
        return primary_df.copy(), {"metric": metric, "threshold": threshold, "skipped": "missing split column"}
    train_df = primary_df[primary_df["split"].astype("string") == "train"]
    ref_df = primary_df[primary_df["split"].astype("string") == "test"]
    cols = [c for c in compare_cols if c in set(primary_df.columns)] if compare_cols else [
        c for c in list(primary_df.columns) if c not in {"split"}
    ]
    per_col: Dict[str, Any] = {}
    drifted: List[str] = []
    for col in cols:
        a = train_df[col].astype("string").value_counts(normalize=True, dropna=False)
        b = ref_df[col].astype("string").value_counts(normalize=True, dropna=False)
        keys = sorted(set(a.index.tolist()) | set(b.index.tolist()))
        score = 0.0
        for k in keys:
            pa = float(a.get(k, 1e-9))
            pb = float(b.get(k, 1e-9))
            if metric == "psi":
                score += (pb - pa) * float((pb / pa) if pa > 0 else 0)
            elif metric == "jsd":
                m = 0.5 * (pa + pb)
                score += 0.5 * abs(pa - m) + 0.5 * abs(pb - m)
            else:
                score = max(score, abs(pa - pb))
        per_col[col] = {"score": float(score)}
        if score > threshold:
            drifted.append(col)
    return primary_df.copy(), {"metric": metric, "threshold": threshold, "driftedColumns": drifted, "perColumn": per_col, "failed": bool(spec.get("failOnDrift", False) and len(drifted) > 0)}


def _execute_determinism_profile_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    normalized = primary_df.copy()
    if bool(spec.get("stableSort", True)) and len(normalized.columns) > 0:
        sort_cols = [str(c) for c in list(normalized.columns)]
        normalized = normalized.sort_values(by=sort_cols, kind="mergesort").reset_index(drop=True)
    payload_hash = hashlib.sha256(df_to_csv_bytes(normalized)).hexdigest()
    return normalized, {"strict": bool(spec.get("strict", True)), "seed": int(spec.get("seed") or 42), "stableSort": bool(spec.get("stableSort", True)), "stableCoercion": bool(spec.get("stableCoercion", True)), "fingerprint": payload_hash}


def _execute_fit_state_registry_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cols_raw = spec.get("includeColumns")
    include_cols = [str(c).strip() for c in cols_raw if str(c).strip()] if isinstance(cols_raw, list) else []
    cols = [c for c in include_cols if c in set(primary_df.columns)] if include_cols else [str(c) for c in list(primary_df.columns)]
    state_payload = {"columns": cols, "dtypes": {c: str(primary_df[c].dtype) for c in cols}}
    state_fingerprint = hashlib.sha256(canonical_json(state_payload).encode("utf-8")).hexdigest()
    return primary_df.copy(), {"mode": str(spec.get("mode") or "fit"), "stateKey": str(spec.get("stateKey") or "default"), "stateFingerprint": state_fingerprint, "state": state_payload}


def _execute_pii_guard_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cols_raw = spec.get("columns")
    selected = [str(c).strip() for c in cols_raw if str(c).strip()] if isinstance(cols_raw, list) else []
    target_cols = [c for c in selected if c in set(primary_df.columns)] if selected else [str(c) for c in list(primary_df.columns)]
    action = str(spec.get("action") or "report").strip().lower()
    out = primary_df.copy()
    email_re = re.compile(r"\b[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}\b")
    phone_re = re.compile(r"\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}\b")
    detected_rows: set[int] = set()
    detections_by_col: Dict[str, int] = {}
    for col in target_cols:
        col_count = 0
        values = []
        for idx, v in enumerate(out[col].tolist()):
            text = "" if v is None else str(v)
            has_pii = bool(email_re.search(text) or phone_re.search(text))
            if has_pii:
                col_count += 1
                detected_rows.add(idx)
                if action == "mask":
                    text = email_re.sub("[EMAIL]", text)
                    text = phone_re.sub("[PHONE]", text)
            values.append(text)
        if action == "mask":
            out[col] = values
        detections_by_col[col] = col_count
    if action == "drop_rows" and detected_rows:
        keep_mask = [i not in detected_rows for i in range(len(out))]
        out = out[keep_mask].reset_index(drop=True)
    report = {
        "columns": target_cols,
        "action": action,
        "detectedRows": int(len(detected_rows)),
        "detectionsByColumn": detections_by_col,
        "failed": bool(spec.get("failOnDetect", False) and len(detected_rows) > 0),
    }
    return out, report


def _execute_inference_parity_op(primary_df: pd.DataFrame, spec: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    train_sig = str(spec.get("trainSignature") or "").strip()
    infer_sig = str(spec.get("inferenceSignature") or "").strip()
    mismatch = bool(train_sig and infer_sig and train_sig != infer_sig)
    report = {
        "trainSignature": train_sig,
        "inferenceSignature": infer_sig,
        "matched": not mismatch,
        "failed": bool(mismatch and bool(spec.get("failOnMismatch", True))),
    }
    return primary_df.copy(), report


def _resolve_dot_path(value: Any, path: str) -> Any:
    current = value
    for raw_part in str(path or "").split("."):
        part = str(raw_part or "").strip()
        if not part:
            raise ValueError(f"Invalid empty path segment in '{path}'")
        if isinstance(current, dict):
            if part not in current:
                raise KeyError(part)
            current = current.get(part)
            continue
        raise KeyError(part)
    return current


def _resolve_filter_value(raw_value: Any, param_config: Dict[str, Any]) -> Any:
    if not isinstance(raw_value, dict) or "valueFrom" not in raw_value:
        return raw_value
    value_from = raw_value.get("valueFrom")
    if not isinstance(value_from, dict):
        raise ValueError("filter valueFrom must be an object")
    handle = str(value_from.get("handle") or "param_config").strip()
    if handle != "param_config":
        raise ValueError("filter valueFrom.handle must equal 'param_config'")
    path = str(value_from.get("path") or "").strip()
    if not path:
        raise ValueError("filter valueFrom.path is required")
    try:
        return _resolve_dot_path(param_config, path)
    except KeyError as ex:
        raise ValueError(f"filter valueFrom.path not found: {path}") from ex


def _compile_filter_condition_sql(
    *,
    condition: Dict[str, Any],
    param_config: Dict[str, Any],
) -> Tuple[str, List[Any], List[str]]:
    col = quote_ident(str(condition.get("column") or "").strip())
    op = str(condition.get("op") or "").strip().lower()
    raw_value = condition.get("value")
    value = _resolve_filter_value(raw_value, param_config) if "value" in condition else None
    used_paths: List[str] = []
    if isinstance(raw_value, dict) and isinstance(raw_value.get("valueFrom"), dict):
        used_paths.append(str((raw_value.get("valueFrom") or {}).get("path") or "").strip())

    if op == "is_null":
        return (f"({col} IS NULL)", [], used_paths)
    if op == "exists":
        return (f"({col} IS NOT NULL)", [], used_paths)
    if op == "not_null":
        return (f"({col} IS NOT NULL)", [], used_paths)

    if op in {"gt", "gte", "lt", "lte"}:
        cmp_op = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
        sql = (
            f"COALESCE(TRY_CAST({col} AS DOUBLE) {cmp_op} TRY_CAST(? AS DOUBLE), FALSE)"
        )
        return sql, [value], used_paths

    if op in {"eq", "ne"}:
        cmp_op = "=" if op == "eq" else "<>"
        if value is None:
            return (f"({col} IS {'NULL' if op == 'eq' else 'NOT NULL'})", [], used_paths)
        if isinstance(value, bool):
            sql = (
                f"COALESCE(TRY_CAST({col} AS BOOLEAN) {cmp_op} TRY_CAST(? AS BOOLEAN), FALSE)"
            )
            return sql, [value], used_paths
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            sql = (
                f"COALESCE(TRY_CAST({col} AS DOUBLE) {cmp_op} TRY_CAST(? AS DOUBLE), FALSE)"
            )
            return sql, [value], used_paths
        sql = f"COALESCE(CAST({col} AS VARCHAR) {cmp_op} CAST(? AS VARCHAR), FALSE)"
        return sql, [str(value)], used_paths

    if op == "contains":
        sql = f"COALESCE(POSITION(CAST(? AS VARCHAR) IN CAST({col} AS VARCHAR)) > 0, FALSE)"
        return sql, [str(value)], used_paths

    if op == "regex":
        sql = f"COALESCE(REGEXP_MATCHES(CAST({col} AS VARCHAR), CAST(? AS VARCHAR)), FALSE)"
        return sql, [str(value)], used_paths

    if op in {"in", "not_in"}:
        if not isinstance(value, list):
            raise ValueError(f"filter op '{op}' requires array value")
        if len(value) == 0:
            return ("FALSE" if op == "in" else "TRUE"), [], used_paths
        placeholders = ", ".join(["CAST(? AS VARCHAR)"] * len(value))
        cmp_keyword = "IN" if op == "in" else "NOT IN"
        sql = f"COALESCE(CAST({col} AS VARCHAR) {cmp_keyword} ({placeholders}), FALSE)"
        return sql, [str(v) for v in value], used_paths

    if op == "between":
        if not isinstance(value, list) or len(value) != 2:
            raise ValueError("filter op 'between' requires array value with 2 entries")
        sql = (
            f"COALESCE(TRY_CAST({col} AS DOUBLE) BETWEEN TRY_CAST(? AS DOUBLE) AND TRY_CAST(? AS DOUBLE), FALSE)"
        )
        return sql, [value[0], value[1]], used_paths

    raise ValueError(f"Unsupported filter op: {op}")


def _compile_filter_rules_to_sql(
    *,
    root: Dict[str, Any],
    param_config: Dict[str, Any],
) -> Tuple[str, List[Any], Dict[str, Any]]:
    used_paths: List[str] = []

    def _compile_node(node: Dict[str, Any]) -> Tuple[str, List[Any]]:
        kind = str(node.get("kind") or "").strip().lower()
        if kind == "group":
            group_op = str(node.get("op") or "all").strip().lower()
            joiner = " AND " if group_op == "all" else " OR "
            conditions = node.get("conditions")
            if not isinstance(conditions, list):
                raise ValueError("filter.rules.conditions must be an array")
            parts: List[str] = []
            bindings: List[Any] = []
            for child in conditions:
                if not isinstance(child, dict):
                    continue
                child_sql, child_bindings = _compile_node(child)
                parts.append(f"({child_sql})")
                bindings.extend(child_bindings)
            if not parts:
                return "TRUE", []
            return joiner.join(parts), bindings
        if kind != "condition":
            raise ValueError("filter.rules node kind must be 'group' or 'condition'")
        sql, bindings, condition_paths = _compile_filter_condition_sql(
            condition=node,
            param_config=param_config,
        )
        for path in condition_paths:
            if path and path not in used_paths:
                used_paths.append(path)
        return sql, bindings

    sql, bindings = _compile_node(root)
    diagnostics = {
        "mode": "rules",
        "whereSql": sql,
        "bindingsCount": len(bindings),
        "paramPaths": used_paths,
    }
    return sql, bindings, diagnostics


def _execute_filter_op(
    primary_df: pd.DataFrame,
    spec: Dict[str, Any],
    *,
    param_config: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    mode = str(spec.get("mode") or "").strip().lower()
    expr = str(spec.get("expr") or "")
    if mode not in {"rules", "sql"}:
        mode = "sql" if expr.strip() else "rules"
    rules = spec.get("rules") if isinstance(spec.get("rules"), dict) else {"kind": "group", "op": "all", "conditions": []}
    param_obj = param_config if isinstance(param_config, dict) else {}

    if duckdb is None:
        raise ModuleNotFoundError("duckdb is required for filter transform operations")

    con = duckdb.connect(database=":memory:")
    try:
        con.register("input", primary_df)
        if mode == "sql":
            if not expr.strip():
                return primary_df, {"mode": "sql", "whereSql": "", "bindingsCount": 0, "paramPaths": []}
            out_df = con.execute(f"select * from input where {expr}").df()
            return out_df, {"mode": "sql", "whereSql": expr, "bindingsCount": 0, "paramPaths": []}

        where_sql, bindings, diagnostics = _compile_filter_rules_to_sql(root=rules, param_config=param_obj)
        out_df = con.execute(f"select * from input where ({where_sql})", bindings).df()
        return out_df, diagnostics
    finally:
        con.close()


JSON_FILTER_COMPARISON_OPS = {"eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "between"}
JSON_FILTER_UNARY_OPS = {"exists", "is_null"}


def _json_filter_condition_path(condition: Dict[str, Any]) -> str:
    path = str(condition.get("path") or "").strip()
    if path:
        return path
    return str(condition.get("column") or "").strip()


def _json_filter_resolve_condition_value(condition: Dict[str, Any], param_config: Dict[str, Any]) -> Any:
    if "value" not in condition:
        return None
    return _resolve_filter_value(condition.get("value"), param_config)


def _json_filter_eval_condition(
    item: Any,
    condition: Dict[str, Any],
    *,
    param_config: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    op = str(condition.get("op") or "").strip().lower()
    path = _json_filter_condition_path(condition)
    if not path:
        return False, {"reason": "missing_path", "path": ""}

    try:
        lhs = _resolve_dot_path(item, path)
        path_exists = True
    except Exception:
        lhs = None
        path_exists = False

    if op == "exists":
        return path_exists, {"path": path, "operator": op}

    if not path_exists:
        return False, {"reason": "missing_path", "path": path, "operator": op}

    if op == "is_null":
        return lhs is None, {"path": path, "operator": op}

    rhs = _json_filter_resolve_condition_value(condition, param_config)

    if op in {"eq", "ne"}:
        ok = lhs == rhs if op == "eq" else lhs != rhs
        return bool(ok), {"path": path, "operator": op, "value": rhs}

    if op in {"gt", "gte", "lt", "lte"}:
        try:
            left_num = float(lhs)
            right_num = float(rhs)
        except Exception:
            return False, {"reason": "type_mismatch", "path": path, "operator": op, "value": rhs}
        if op == "gt":
            ok = left_num > right_num
        elif op == "gte":
            ok = left_num >= right_num
        elif op == "lt":
            ok = left_num < right_num
        else:
            ok = left_num <= right_num
        return bool(ok), {"path": path, "operator": op, "value": rhs}

    if op == "in":
        if not isinstance(rhs, list):
            return False, {"reason": "type_mismatch", "path": path, "operator": op, "value": rhs}
        return lhs in rhs, {"path": path, "operator": op, "value": rhs}

    if op == "contains":
        if isinstance(lhs, str):
            return str(rhs) in lhs, {"path": path, "operator": op, "value": rhs}
        if isinstance(lhs, list):
            return rhs in lhs, {"path": path, "operator": op, "value": rhs}
        return False, {"reason": "type_mismatch", "path": path, "operator": op, "value": rhs}

    if op == "between":
        if not isinstance(rhs, list) or len(rhs) != 2:
            return False, {"reason": "type_mismatch", "path": path, "operator": op, "value": rhs}
        try:
            left_num = float(lhs)
            lo = float(rhs[0])
            hi = float(rhs[1])
        except Exception:
            return False, {"reason": "type_mismatch", "path": path, "operator": op, "value": rhs}
        return lo <= left_num <= hi, {"path": path, "operator": op, "value": rhs}

    return False, {"reason": "unsupported_operator", "path": path, "operator": op}


def _json_filter_eval_node(
    item: Any,
    node: Dict[str, Any],
    *,
    param_config: Dict[str, Any],
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    kind = str(node.get("kind") or "").strip().lower()
    if kind == "condition":
        ok, details = _json_filter_eval_condition(item, node, param_config=param_config)
        if ok:
            return True, None
        return False, details

    if kind != "group":
        return False, {"reason": "invalid_rule", "operator": "group"}

    group_op = str(node.get("op") or "all").strip().lower()
    conditions = node.get("conditions") if isinstance(node.get("conditions"), list) else []
    if not conditions:
        return True, None

    if group_op == "any":
        first_reason: Optional[Dict[str, Any]] = None
        for child in conditions:
            if not isinstance(child, dict):
                continue
            ok, reason = _json_filter_eval_node(item, child, param_config=param_config)
            if ok:
                return True, None
            if first_reason is None and isinstance(reason, dict):
                first_reason = reason
        return False, first_reason or {"reason": "no_match"}

    # default all
    for child in conditions:
        if not isinstance(child, dict):
            continue
        ok, reason = _json_filter_eval_node(item, child, param_config=param_config)
        if not ok:
            return False, reason or {"reason": "no_match"}
    return True, None


def _execute_json_filter_op(
    input_value: Any,
    spec: Dict[str, Any],
    *,
    param_config: Optional[Dict[str, Any]] = None,
) -> Tuple[Any, Any, Dict[str, Any]]:
    root = spec.get("rules") if isinstance(spec.get("rules"), dict) else {"kind": "group", "op": "all", "conditions": []}
    include_reject_meta = bool(spec.get("include_reject_meta", True))
    route_reject = bool(spec.get("route_reject", True))
    param_obj = param_config if isinstance(param_config, dict) else {}

    is_list_input = isinstance(input_value, list)
    items = input_value if isinstance(input_value, list) else [input_value]
    passed: List[Any] = []
    rejected: List[Any] = []
    reject_reasons: List[str] = []
    for idx, item in enumerate(items):
        ok, reason = _json_filter_eval_node(item, root, param_config=param_obj)
        if ok:
            passed.append(item)
            continue
        reason_obj = reason if isinstance(reason, dict) else {"reason": "rejected"}
        reason_code = str(reason_obj.get("reason") or "rejected")
        reject_reasons.append(reason_code)
        if include_reject_meta and isinstance(item, dict):
            reject_item = dict(item)
            reject_item["_reject"] = {
                "reason": reason_code,
                "rule": reason_obj.get("rule"),
                "path": reason_obj.get("path"),
                "operator": reason_obj.get("operator"),
                "index": idx,
            }
            rejected.append(reject_item)
        elif include_reject_meta:
            rejected.append({"value": item, "_reject": {"reason": reason_code, "index": idx}})
        else:
            rejected.append(item)

    pass_payload: Any
    if is_list_input:
        pass_payload = passed
    else:
        pass_payload = passed[0] if passed else []

    reject_payload: Any = rejected if route_reject else []
    diagnostics = {
        "mode": "rules",
        "passed": len(passed),
        "rejected": len(rejected),
        "rejectReasons": sorted(_stable_unique_strings(reject_reasons)),
    }
    return pass_payload, reject_payload, diagnostics


def _resolve_derive_arg_value(raw_arg: Any, param_config: Dict[str, Any]) -> Tuple[str, List[Any], List[str]]:
    if isinstance(raw_arg, dict):
        if isinstance(raw_arg.get("column"), str) and str(raw_arg.get("column")).strip():
            return quote_ident(str(raw_arg.get("column")).strip()), [], []
        if isinstance(raw_arg.get("valueFrom"), dict):
            value_from = raw_arg.get("valueFrom")
            handle = str(value_from.get("handle") or "param_config").strip()
            if handle != "param_config":
                raise ValueError("derive formula valueFrom.handle must equal 'param_config'")
            path = str(value_from.get("path") or "").strip()
            if not path:
                raise ValueError("derive formula valueFrom.path is required")
            try:
                resolved = _resolve_dot_path(param_config, path)
            except KeyError as ex:
                raise ValueError(f"derive formula valueFrom.path not found: {path}") from ex
            return "?", [resolved], [path]
    return "?", [raw_arg], []


def _compile_derive_formula_sql(
    formula: Dict[str, Any],
    *,
    param_config: Dict[str, Any],
) -> Tuple[str, List[Any], List[str]]:
    op = str(formula.get("op") or "").strip().lower()
    args = formula.get("args")
    if not isinstance(args, list):
        raise ValueError("derive formula args must be an array")

    sql_args: List[str] = []
    bindings: List[Any] = []
    used_paths: List[str] = []
    for raw_arg in args:
        arg_sql, arg_bindings, arg_paths = _resolve_derive_arg_value(raw_arg, param_config)
        sql_args.append(arg_sql)
        bindings.extend(arg_bindings)
        for path in arg_paths:
            if path and path not in used_paths:
                used_paths.append(path)

    if op == "add":
        return (
            f"(TRY_CAST({sql_args[0]} AS DOUBLE) + TRY_CAST({sql_args[1]} AS DOUBLE))",
            bindings,
            used_paths,
        )
    if op == "sub":
        return (
            f"(TRY_CAST({sql_args[0]} AS DOUBLE) - TRY_CAST({sql_args[1]} AS DOUBLE))",
            bindings,
            used_paths,
        )
    if op == "mul":
        return (
            f"(TRY_CAST({sql_args[0]} AS DOUBLE) * TRY_CAST({sql_args[1]} AS DOUBLE))",
            bindings,
            used_paths,
        )
    if op == "div":
        return (
            f"(TRY_CAST({sql_args[0]} AS DOUBLE) / NULLIF(TRY_CAST({sql_args[1]} AS DOUBLE), 0))",
            bindings,
            used_paths,
        )
    if op == "concat":
        expr = " || ".join([f"COALESCE(CAST({arg} AS VARCHAR), '')" for arg in sql_args])
        return f"({expr})", bindings, used_paths
    if op == "lower":
        return f"LOWER(CAST({sql_args[0]} AS VARCHAR))", bindings, used_paths
    if op == "upper":
        return f"UPPER(CAST({sql_args[0]} AS VARCHAR))", bindings, used_paths
    if op == "trim":
        return f"TRIM(CAST({sql_args[0]} AS VARCHAR))", bindings, used_paths
    if op == "length":
        return f"LENGTH(CAST({sql_args[0]} AS VARCHAR))", bindings, used_paths
    if op == "coalesce":
        return f"COALESCE({', '.join(sql_args)})", bindings, used_paths
    raise ValueError(f"Unsupported derive formula op: {op}")


def _execute_derive_op(
    primary_df: pd.DataFrame,
    spec: Dict[str, Any],
    *,
    param_config: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    mode = str(spec.get("mode") or "").strip().lower()
    columns = spec.get("columns") if isinstance(spec.get("columns"), list) else []
    rules = spec.get("rules") if isinstance(spec.get("rules"), list) else []
    has_sql_expr = any(
        isinstance(item, dict) and str(item.get("expr") or "").strip()
        for item in columns
    )
    if mode not in {"rules", "sql"}:
        mode = "sql" if has_sql_expr else "rules"
    param_obj = param_config if isinstance(param_config, dict) else {}

    if duckdb is None:
        raise ModuleNotFoundError("duckdb is required for derive transform operations")

    con = duckdb.connect(database=":memory:")
    try:
        con.register("input", primary_df)
        source_cols = [quote_ident(c) for c in list(primary_df.columns)]
        if mode == "sql":
            select_parts = list(source_cols)
            for col in columns:
                if not isinstance(col, dict):
                    continue
                name = str(col.get("name") or "").strip()
                expr = str(col.get("expr") or "").strip()
                if not name or not expr:
                    continue
                select_parts.append(f"({expr}) as {quote_ident(name)}")
            sql_query = f"select {', '.join(select_parts)} from input"
            out_df = con.execute(sql_query).df()
            return out_df, {
                "mode": "sql",
                "selectSql": sql_query,
                "bindingsCount": 0,
                "paramPaths": [],
            }

        select_parts = list(source_cols)
        bindings: List[Any] = []
        used_paths: List[str] = []
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            name = str(rule.get("name") or "").strip()
            formula = rule.get("formula")
            if not name or not isinstance(formula, dict):
                continue
            expr_sql, expr_bindings, expr_paths = _compile_derive_formula_sql(
                formula,
                param_config=param_obj,
            )
            select_parts.append(f"({expr_sql}) as {quote_ident(name)}")
            bindings.extend(expr_bindings)
            for path in expr_paths:
                if path and path not in used_paths:
                    used_paths.append(path)
        sql_query = f"select {', '.join(select_parts)} from input"
        out_df = con.execute(sql_query, bindings).df()
        return out_df, {
            "mode": "rules",
            "selectSql": sql_query,
            "bindingsCount": len(bindings),
            "paramPaths": used_paths,
        }
    finally:
        con.close()

# ---- duckdb execution ----

def execute_transform_op(
    op: str,
    params: Dict[str, Any],
    inputs: Dict[str, pd.DataFrame],
    join_lookup: Optional[Dict[str, pd.DataFrame]] = None,
    param_inputs: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Execute via DuckDB to keep semantics consistent.
    Convention:
    - primary input table is inputs["in"] (or first input)
    - for join, params.join.clauses drives node-qualified joins via join_lookup[nodeId]
    """
    primary_df = inputs.get("in")
    if primary_df is None:
        if not inputs:
            raise ValueError("Transform has no input tables (expected at least one).")
        primary_df = next(iter(inputs.values()))

    if op == "split":
        spec = params["split"]
        return _execute_split_op(primary_df, spec)
    if op == "dedupe":
        spec = params["dedupe"]
        return _execute_dedupe_op(primary_df, spec)
    if op == "null_policy":
        spec = params["null_policy"]
        out_df, _ = _execute_null_policy_op(primary_df, spec)
        return out_df
    if op == "outlier_policy":
        spec = params["outlier_policy"]
        out_df, _ = _execute_outlier_policy_op(primary_df, spec)
        return out_df
    if op == "text_clean":
        spec = params["text_clean"]
        out_df, _ = _execute_text_clean_op(primary_df, spec)
        return out_df
    if op == "nlp_normalize":
        spec = params["nlp_normalize"]
        out_df, _ = _execute_nlp_normalize_op(primary_df, spec)
        return out_df
    if op == "tokenize_chunk":
        spec = params["tokenize_chunk"]
        out_df, _ = _execute_tokenize_chunk_op(primary_df, spec)
        return out_df
    if op == "dataset_split":
        spec = params["dataset_split"]
        out_df, _ = _execute_dataset_split_op(primary_df, spec)
        return out_df
    if op == "class_imbalance":
        spec = params["class_imbalance"]
        out_df, _ = _execute_class_imbalance_op(primary_df, spec)
        return out_df
    if op == "categorical_encode":
        spec = params["categorical_encode"]
        out_df, _ = _execute_categorical_encode_op(primary_df, spec)
        return out_df
    if op == "numeric_scale":
        spec = params["numeric_scale"]
        out_df, _ = _execute_numeric_scale_op(primary_df, spec)
        return out_df
    if op == "embedding":
        spec = params["embedding"]
        out_df, _ = _execute_embedding_op(primary_df, spec)
        return out_df
    if op == "feature_selection":
        spec = params["feature_selection"]
        out_df, _ = _execute_feature_selection_op(primary_df, spec)
        return out_df
    if op == "leakage_detect":
        spec = params["leakage_detect"]
        out_df, _ = _execute_leakage_detect_op(primary_df, spec)
        return out_df
    if op == "quality_profile":
        spec = params["quality_profile"]
        out_df, _ = _execute_quality_profile_op(primary_df, spec)
        return out_df
    if op == "drift_compare":
        spec = params["drift_compare"]
        out_df, _ = _execute_drift_compare_op(primary_df, spec)
        return out_df
    if op == "determinism_profile":
        spec = params["determinism_profile"]
        out_df, _ = _execute_determinism_profile_op(primary_df, spec)
        return out_df
    if op == "fit_state_registry":
        spec = params["fit_state_registry"]
        out_df, _ = _execute_fit_state_registry_op(primary_df, spec)
        return out_df
    if op == "pii_guard":
        spec = params["pii_guard"]
        out_df, _ = _execute_pii_guard_op(primary_df, spec)
        return out_df
    if op == "inference_parity":
        spec = params["inference_parity"]
        out_df, _ = _execute_inference_parity_op(primary_df, spec)
        return out_df
    if op == "aggregate":
        spec = params["aggregate"]
        return _execute_aggregate_op(primary_df, spec)
    if op == "quality_gate":
        spec = params["quality_gate"]
        return _execute_quality_gate_op(primary_df, spec)
    if op == "ml_contract":
        return primary_df
    if op == "json_to_table":
        return primary_df
    if op == "text_to_table":
        spec = params.get("text_to_table") if isinstance(params.get("text_to_table"), dict) else {}
        mode = str(spec.get("mode") or "lines").strip().lower()
        if mode == "lines":
            column = str(spec.get("column") or "text").strip() or "text"
            if list(primary_df.columns) == [column]:
                return primary_df
            if "text" in primary_df.columns:
                return primary_df.rename(columns={"text": column})
            if len(primary_df.columns) == 1:
                return primary_df.rename(columns={str(primary_df.columns[0]): column})
            return primary_df
        return primary_df
    if op == "table_to_json":
        return primary_df

    if duckdb is None:
        raise ModuleNotFoundError("duckdb is required for non-split transform operations")

    con = duckdb.connect(database=":memory:")
    try:
        # register input tables
        primary_name = "input"

        con.register(primary_name, primary_df)

        if op == "filter":
            spec = params["filter"] if isinstance(params.get("filter"), dict) else {}
            out_df, _ = _execute_filter_op(
                primary_df,
                spec,
                param_config=(param_inputs or {}).get("param_config") if isinstance(param_inputs, dict) else None,
            )
            return out_df

        elif op == "select":
            spec = params["select"]
            mode = str(spec.get("mode") or "include").strip().lower()
            keep_order = str(spec.get("keepOrder") or ("input" if mode == "exclude" else "custom")).strip().lower()
            cols = [str(c) for c in (spec.get("columns") or [])]
            input_cols = [str(c) for c in list(primary_df.columns)]
            input_set = set(input_cols)
            selected_set = set(cols)
            if mode == "exclude":
                out_cols = [c for c in input_cols if c not in selected_set]
            elif keep_order == "input":
                out_cols = [c for c in input_cols if c in selected_set]
            else:
                out_cols = [c for c in cols if c in input_set]
            if not out_cols:
                return primary_df.iloc[:, 0:0].copy()
            col_sql = ", ".join([quote_ident(c) for c in out_cols])
            return con.execute(f"select {col_sql} from {primary_name}").df()

        elif op == "rename":
            mp = params["rename"]["map"]
            # SELECT col AS new_name for mapped, else keep as-is
            cols = list(primary_df.columns)
            parts = []
            for c in cols:
                if c in mp:
                    parts.append(f"{quote_ident(c)} as {quote_ident(mp[c])}")
                else:
                    parts.append(f"{quote_ident(c)}")
            return con.execute(f"select {', '.join(parts)} from {primary_name}").df()

        elif op == "derive":
            spec = params["derive"] if isinstance(params.get("derive"), dict) else {}
            out_df, _ = _execute_derive_op(
                primary_df,
                spec,
                param_config=(param_inputs or {}).get("param_config") if isinstance(param_inputs, dict) else None,
            )
            return out_df

        elif op == "join":
            if not join_lookup:
                raise ValueError("join_lookup missing for join op")
            spec = params["join"]
            clauses = spec.get("clauses") or []
            if not isinstance(clauses, list) or len(clauses) == 0:
                raise ValueError("join.clauses must be a non-empty array")

            joined_nodes: set[str] = set()
            alias_map: Dict[str, str] = {}
            alias_index = 0
            sql_from: Optional[str] = None

            def ensure_alias(node_id: str) -> str:
                nonlocal alias_index
                if node_id in alias_map:
                    return alias_map[node_id]
                df = join_lookup.get(node_id)
                if df is None:
                    raise ValueError(f"join clause references unknown node '{node_id}'")
                alias = f"t{alias_index}"
                alias_index += 1
                alias_map[node_id] = alias
                con.register(alias, df)
                return alias

            for idx, clause in enumerate(clauses):
                if not isinstance(clause, dict):
                    raise ValueError(f"join clause at index {idx} is invalid")
                left_node = str(clause.get("leftNodeId") or "").strip()
                right_node = str(clause.get("rightNodeId") or "").strip()
                left_col = str(clause.get("leftCol") or "").strip()
                right_col = str(clause.get("rightCol") or "").strip()
                how = str(clause.get("how") or "inner").strip().lower()
                if how not in JOIN_HOWS:
                    how = "inner"
                how_sql = {"inner": "inner", "left": "left", "right": "right", "full": "full outer"}[how]
                if not left_node or not right_node or not left_col or not right_col:
                    raise ValueError(f"join clause at index {idx} has empty node/column values")

                left_alias = ensure_alias(left_node)
                right_alias = ensure_alias(right_node)

                if sql_from is None:
                    sql_from = left_alias
                    joined_nodes.add(left_node)

                left_in = left_node in joined_nodes
                right_in = right_node in joined_nodes

                if not left_in and not right_in:
                    raise ValueError(
                        "join clauses must form a connected chain; "
                        f"clause {idx} references two unjoined nodes ({left_node}, {right_node})"
                    )
                if left_in and right_in:
                    raise ValueError(
                        "join clauses must add exactly one new node at each step; "
                        f"clause {idx} references two already-joined nodes ({left_node}, {right_node})"
                    )

                if left_in:
                    sql_from = (
                        f"{sql_from} {how_sql} join {right_alias} "
                        f"on {left_alias}.{quote_ident(left_col)} = {right_alias}.{quote_ident(right_col)}"
                    )
                    joined_nodes.add(right_node)
                else:
                    sql_from = (
                        f"{sql_from} {how_sql} join {left_alias} "
                        f"on {left_alias}.{quote_ident(left_col)} = {right_alias}.{quote_ident(right_col)}"
                    )
                    joined_nodes.add(left_node)

            if not sql_from:
                raise ValueError("join clauses did not produce a valid join plan")
            return con.execute(f"select * from {sql_from}").df()

        elif op == "sort":
            by = params["sort"]["by"]
            order_sql = ", ".join([f"{quote_ident(x['col'])} {x['dir'].upper()}" for x in by])
            return con.execute(f"select * from {primary_name} order by {order_sql}").df()

        elif op == "limit":
            n = int(params["limit"]["n"])
            return con.execute(f"select * from {primary_name} limit {n}").df()

        elif op == "dedupe":
            by = params["dedupe"].get("by")

            # columns to order by for deterministic “first row”
            all_cols = list(primary_df.columns)
            order_sql = ", ".join([quote_ident(c) for c in all_cols])

            if by:
                part_sql = ", ".join([quote_ident(c) for c in by])
                q = f"""
                select * exclude (rn)
                from (
                    select *,
                        row_number() over (
                            partition by {part_sql}
                            order by {order_sql}
                        ) as rn
                    from {primary_name}
                )
                where rn = 1
                """
                return con.execute(q).df()

            # no 'by' => deterministic unique set; DISTINCT doesn’t guarantee order, so sort it
            q = f"select distinct * from {primary_name} order by {order_sql}"
            return con.execute(q).df()

        elif op == "sql":
            q = params["sql"]["query"]
            # convention: user writes SQL referencing "input" (and optionally "other" if you add)
            return con.execute(q).df()

        raise ValueError(f"Unsupported transform op: {op}")
    finally:
        con.close()

def run_transform(
    *,
    params: Dict[str, Any],
    input_tables: Dict[str, pd.DataFrame],
    join_lookup: Optional[Dict[str, pd.DataFrame]],
    param_inputs: Optional[Dict[str, Any]] = None,
) -> TransformResult:
    t0 = time.perf_counter()
    op = params["op"]
    primary_input = input_tables.get("in")
    if primary_input is None:
        primary_input = next(iter(input_tables.values())) if input_tables else pd.DataFrame()
    quality_gate_report: Optional[Dict[str, Any]] = None
    null_policy_report: Optional[Dict[str, Any]] = None
    outlier_policy_report: Optional[Dict[str, Any]] = None
    text_clean_report: Optional[Dict[str, Any]] = None
    nlp_normalize_report: Optional[Dict[str, Any]] = None
    tokenize_chunk_report: Optional[Dict[str, Any]] = None
    dataset_split_report: Optional[Dict[str, Any]] = None
    class_imbalance_report: Optional[Dict[str, Any]] = None
    derive_compile_report: Optional[Dict[str, Any]] = None
    categorical_encode_report: Optional[Dict[str, Any]] = None
    numeric_scale_report: Optional[Dict[str, Any]] = None
    embedding_report: Optional[Dict[str, Any]] = None
    feature_selection_report: Optional[Dict[str, Any]] = None
    filter_compile_report: Optional[Dict[str, Any]] = None
    json_filter_compile_report: Optional[Dict[str, Any]] = None
    leakage_detect_report: Optional[Dict[str, Any]] = None
    quality_profile_report: Optional[Dict[str, Any]] = None
    drift_compare_report: Optional[Dict[str, Any]] = None
    determinism_profile_report: Optional[Dict[str, Any]] = None
    fit_state_registry_report: Optional[Dict[str, Any]] = None
    pii_guard_report: Optional[Dict[str, Any]] = None
    inference_parity_report: Optional[Dict[str, Any]] = None
    if op == "filter":
        spec = params["filter"] if isinstance(params.get("filter"), dict) else {}
        out_df, filter_compile_report = _execute_filter_op(
            primary_input,
            spec,
            param_config=(param_inputs or {}).get("param_config") if isinstance(param_inputs, dict) else None,
        )
    elif op == "json_filter":
        spec = params["json_filter"] if isinstance(params.get("json_filter"), dict) else {}
        filter_input_value = None
        if isinstance(param_inputs, dict):
            filter_input_value = param_inputs.get("_json_filter_in")
        pass_payload, reject_payload, json_filter_compile_report = _execute_json_filter_op(
            filter_input_value,
            spec,
            param_config=(param_inputs or {}).get("param_config") if isinstance(param_inputs, dict) else None,
        )
        pass_bytes = canonical_json(pass_payload).encode("utf-8")
        reject_bytes = canonical_json(reject_payload).encode("utf-8")
        meta = {
            "content_hash": sha256_hex(pass_bytes),
            "format": "json",
            "payloadType": "json",
            "json_filter_compile": json_filter_compile_report or {},
            "row_count": len(pass_payload) if isinstance(pass_payload, list) else (1 if pass_payload not in (None, [], {}) else 0),
            "columns": [],
        }
        reject_meta = {
            "content_hash": sha256_hex(reject_bytes),
            "format": "json",
            "payloadType": "json",
            "json_filter_compile": json_filter_compile_report or {},
            "row_count": len(reject_payload) if isinstance(reject_payload, list) else (1 if reject_payload not in (None, [], {}) else 0),
            "columns": [],
            "is_reject_output": True,
        }
        additional = {
            "out_reject": TransformAdditionalOutput(
                payload_bytes=reject_bytes,
                mime_type="application/json; charset=utf-8",
                meta=reject_meta,
            )
        }
        return TransformResult(
            payload_bytes=pass_bytes,
            mime_type="application/json; charset=utf-8",
            meta=meta,
            additional_outputs=additional,
        )
    elif op == "derive":
        spec = params["derive"] if isinstance(params.get("derive"), dict) else {}
        out_df, derive_compile_report = _execute_derive_op(
            primary_input,
            spec,
            param_config=(param_inputs or {}).get("param_config") if isinstance(param_inputs, dict) else None,
        )
    elif op == "quality_gate":
        spec = params["quality_gate"]
        primary_df = input_tables.get("in")
        if primary_df is None:
            if not input_tables:
                raise ValueError("Transform has no input tables (expected at least one).")
            primary_df = next(iter(input_tables.values()))
        quality_gate_report = _quality_gate_report(primary_df, spec)
        if bool(quality_gate_report.get("failed")):
            raise ValueError(_quality_gate_failure_message(quality_gate_report))
        out_df = primary_df
    elif op == "null_policy":
        spec = params["null_policy"]
        out_df, null_policy_report = _execute_null_policy_op(primary_input, spec)
    elif op == "outlier_policy":
        spec = params["outlier_policy"]
        out_df, outlier_policy_report = _execute_outlier_policy_op(primary_input, spec)
    elif op == "text_clean":
        spec = params["text_clean"]
        out_df, text_clean_report = _execute_text_clean_op(primary_input, spec)
    elif op == "nlp_normalize":
        spec = params["nlp_normalize"]
        out_df, nlp_normalize_report = _execute_nlp_normalize_op(primary_input, spec)
    elif op == "tokenize_chunk":
        spec = params["tokenize_chunk"]
        out_df, tokenize_chunk_report = _execute_tokenize_chunk_op(primary_input, spec)
    elif op == "dataset_split":
        spec = params["dataset_split"]
        out_df, dataset_split_report = _execute_dataset_split_op(primary_input, spec)
    elif op == "class_imbalance":
        spec = params["class_imbalance"]
        out_df, class_imbalance_report = _execute_class_imbalance_op(primary_input, spec)
    elif op == "categorical_encode":
        spec = params["categorical_encode"]
        out_df, categorical_encode_report = _execute_categorical_encode_op(primary_input, spec)
    elif op == "numeric_scale":
        spec = params["numeric_scale"]
        out_df, numeric_scale_report = _execute_numeric_scale_op(primary_input, spec)
    elif op == "embedding":
        spec = params["embedding"]
        out_df, embedding_report = _execute_embedding_op(primary_input, spec)
    elif op == "feature_selection":
        spec = params["feature_selection"]
        out_df, feature_selection_report = _execute_feature_selection_op(primary_input, spec)
    elif op == "leakage_detect":
        spec = params["leakage_detect"]
        out_df, leakage_detect_report = _execute_leakage_detect_op(primary_input, spec)
        if bool((leakage_detect_report or {}).get("violated")):
            raise ValueError("leakage_detect failed: overlap exceeds configured maxAllowedOverlap")
    elif op == "quality_profile":
        spec = params["quality_profile"]
        out_df, quality_profile_report = _execute_quality_profile_op(primary_input, spec)
    elif op == "drift_compare":
        spec = params["drift_compare"]
        out_df, drift_compare_report = _execute_drift_compare_op(primary_input, spec)
        if bool((drift_compare_report or {}).get("failed")):
            raise ValueError("drift_compare failed: drift exceeds configured threshold")
    elif op == "determinism_profile":
        spec = params["determinism_profile"]
        out_df, determinism_profile_report = _execute_determinism_profile_op(primary_input, spec)
    elif op == "fit_state_registry":
        spec = params["fit_state_registry"]
        out_df, fit_state_registry_report = _execute_fit_state_registry_op(primary_input, spec)
    elif op == "pii_guard":
        spec = params["pii_guard"]
        out_df, pii_guard_report = _execute_pii_guard_op(primary_input, spec)
        if bool((pii_guard_report or {}).get("failed")):
            raise ValueError("pii_guard failed: PII detected and failOnDetect=true")
    elif op == "inference_parity":
        spec = params["inference_parity"]
        out_df, inference_parity_report = _execute_inference_parity_op(primary_input, spec)
        if bool((inference_parity_report or {}).get("failed")):
            raise ValueError("inference_parity failed: train/inference signatures mismatch")
    else:
        out_df = execute_transform_op(
            op,
            params,
            input_tables,
            join_lookup=join_lookup,
            param_inputs=param_inputs,
        )

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    execution_meta = _build_execution_metadata(
        op=op,
        params=params,
        input_df=primary_input,
        output_df=out_df,
        elapsed_ms=elapsed_ms,
    )

    if op == "table_to_json":
        spec = params.get("table_to_json") if isinstance(params.get("table_to_json"), dict) else {}
        orient = str(spec.get("orient") or "records").strip().lower()
        pretty = bool(spec.get("pretty", False))
        payload = df_to_json_bytes(out_df, orient=orient, pretty=pretty)
        meta = {
            "row_count": int(len(out_df)),
            "columns": list(out_df.columns),
            "content_hash": sha256_hex(payload),
            "format": "json",
            "payloadType": "json",
            "execution": execution_meta,
        }
        if quality_gate_report is not None:
            meta["quality_gate"] = quality_gate_report
        return TransformResult(payload_bytes=payload, mime_type="application/json; charset=utf-8", meta=meta)

    payload = df_to_csv_bytes(out_df)
    meta = {
        "row_count": int(len(out_df)),
        "columns": list(out_df.columns),
        "content_hash": sha256_hex(payload),
        "format": "csv",
        "payloadType": "table",
        "execution": execution_meta,
    }
    if quality_gate_report is not None:
        meta["quality_gate"] = quality_gate_report
    if null_policy_report is not None:
        meta["null_policy"] = null_policy_report
    if outlier_policy_report is not None:
        meta["outlier_policy"] = outlier_policy_report
    if text_clean_report is not None:
        meta["text_clean"] = text_clean_report
    if nlp_normalize_report is not None:
        meta["nlp_normalize"] = nlp_normalize_report
    if tokenize_chunk_report is not None:
        meta["tokenize_chunk"] = tokenize_chunk_report
    if dataset_split_report is not None:
        meta["dataset_split"] = dataset_split_report
    if class_imbalance_report is not None:
        meta["class_imbalance"] = class_imbalance_report
    if categorical_encode_report is not None:
        meta["categorical_encode"] = categorical_encode_report
    if numeric_scale_report is not None:
        meta["numeric_scale"] = numeric_scale_report
    if embedding_report is not None:
        meta["embedding"] = embedding_report
    if feature_selection_report is not None:
        meta["feature_selection"] = feature_selection_report
    if leakage_detect_report is not None:
        meta["leakage_detect"] = leakage_detect_report
    if quality_profile_report is not None:
        meta["quality_profile"] = quality_profile_report
    if drift_compare_report is not None:
        meta["drift_compare"] = drift_compare_report
    if determinism_profile_report is not None:
        meta["determinism_profile"] = determinism_profile_report
    if fit_state_registry_report is not None:
        meta["fit_state_registry"] = fit_state_registry_report
    if pii_guard_report is not None:
        meta["pii_guard"] = pii_guard_report
    if inference_parity_report is not None:
        meta["inference_parity"] = inference_parity_report
    if filter_compile_report is not None:
        meta["filter_compile"] = filter_compile_report
    if derive_compile_report is not None:
        meta["derive_compile"] = derive_compile_report
    if op == "ml_contract":
        contract_spec = params.get("ml_contract") if isinstance(params.get("ml_contract"), dict) else {}
        meta["ml_contract"] = {
            "taskType": str(contract_spec.get("taskType") or "other"),
            "labelColumn": str(contract_spec.get("labelColumn") or ""),
            "featureColumns": [str(c) for c in (contract_spec.get("featureColumns") or []) if str(c).strip()],
            "idColumn": str(contract_spec.get("idColumn") or ""),
            "timestampColumn": str(contract_spec.get("timestampColumn") or ""),
            "allowExtraFeatures": bool(contract_spec.get("allowExtraFeatures", True)),
            "requireNonNullLabel": bool(contract_spec.get("requireNonNullLabel", True)),
        }
    return TransformResult(payload_bytes=payload, mime_type="text/csv; charset=utf-8", meta=meta)

