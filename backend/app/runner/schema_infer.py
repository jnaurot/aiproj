from __future__ import annotations

import json
from collections import OrderedDict
from dataclasses import dataclass
from hashlib import sha256
from threading import Lock
from typing import Any, Dict, Tuple


def _json_schema_sort_key(schema: Dict[str, Any]) -> str:
    try:
        return json.dumps(schema, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        return str(schema)


def _json_payload_value_schema(value: Any, *, depth: int = 0, max_depth: int = 24) -> Dict[str, Any]:
    if depth >= max_depth:
        return {"type": "unknown", "reason": "max_depth"}

    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, int):
        return {"type": "integer"}
    if isinstance(value, float):
        return {"type": "number"}
    if isinstance(value, str):
        return {"type": "string"}

    if isinstance(value, dict):
        props: Dict[str, Any] = {}
        required: list[str] = []
        for raw_key in sorted(value.keys(), key=lambda k: str(k)):
            key = str(raw_key)
            props[key] = _json_payload_value_schema(value[raw_key], depth=depth + 1, max_depth=max_depth)
            required.append(key)
        return {
            "type": "object",
            "properties": props,
            "required": required,
            "additionalProperties": False,
        }

    if isinstance(value, list):
        if not value:
            return {"type": "array", "items": {"type": "unknown"}}

        unique: Dict[str, Dict[str, Any]] = {}
        for item in value:
            item_schema = _json_payload_value_schema(item, depth=depth + 1, max_depth=max_depth)
            unique[_json_schema_sort_key(item_schema)] = item_schema

        ordered = [unique[k] for k in sorted(unique.keys())]
        if len(ordered) == 1:
            items_schema: Dict[str, Any] = ordered[0]
        else:
            items_schema = {"type": "union", "anyOf": ordered}

        return {"type": "array", "items": items_schema}

    return {"type": "unknown"}


def _canonical_json(value: Any) -> Tuple[str, bool]:
    try:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return payload, True
    except Exception:
        # Non-JSON-serializable payloads are inferred without memoization.
        return repr(value), False


@dataclass
class _SchemaInferStats:
    hit: int = 0
    miss: int = 0
    bypass: int = 0
    entries: int = 0


class _SchemaInferMemo:
    def __init__(self, max_size: int = 512) -> None:
        self._max_size = max(32, int(max_size))
        self._lock = Lock()
        self._cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._stats = _SchemaInferStats()

    def infer(self, value: Any, *, max_depth: int = 24) -> Dict[str, Any]:
        payload, cacheable = _canonical_json(value)
        if not cacheable:
            with self._lock:
                self._stats.bypass += 1
            return _json_payload_value_schema(value, max_depth=max_depth)

        digest = sha256(payload.encode("utf-8")).hexdigest()
        with self._lock:
            found = self._cache.get(digest)
            if found is not None:
                self._cache.move_to_end(digest)
                self._stats.hit += 1
                return json.loads(json.dumps(found))

        parsed = json.loads(payload)
        inferred = _json_payload_value_schema(parsed, max_depth=max_depth)
        with self._lock:
            self._stats.miss += 1
            self._cache[digest] = inferred
            self._cache.move_to_end(digest)
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)
            self._stats.entries = len(self._cache)
        return json.loads(json.dumps(inferred))

    def stats(self) -> Dict[str, int]:
        with self._lock:
            self._stats.entries = len(self._cache)
            return {
                "hit": int(self._stats.hit),
                "miss": int(self._stats.miss),
                "bypass": int(self._stats.bypass),
                "entries": int(self._stats.entries),
            }


_SCHEMA_INFER_MEMO = _SchemaInferMemo()


def infer_json_schema_cached(value: Any, *, max_depth: int = 24) -> Dict[str, Any]:
    return _SCHEMA_INFER_MEMO.infer(value, max_depth=max_depth)


def get_schema_infer_stats() -> Dict[str, int]:
    return _SCHEMA_INFER_MEMO.stats()
