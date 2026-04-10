"""
Memoization fingerprint computation for DAG nodes.

A node's MemoKey is a SHA-256 hash of:
  - node kind
  - canonical JSON of node params (keys sorted, floats normalized)
  - sorted list of input artifact IDs from upstream nodes
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional, Sequence


def _canonical_params(params: Any) -> str:
	"""Produce a deterministic JSON string from params."""

	def _normalize(value: Any) -> Any:
		if isinstance(value, float):
			# Avoid cache misses from floating-point representation noise.
			return float(f"{value:.8g}")
		if isinstance(value, dict):
			return {k: _normalize(v) for k, v in sorted(value.items())}
		if isinstance(value, (list, tuple)):
			return [_normalize(v) for v in value]
		return value

	return json.dumps(_normalize(params), sort_keys=True, separators=(",", ":"))


def compute_memo_key(
	node_kind: str,
	params: Any,
	input_artifact_ids: Sequence[str],
) -> str:
	"""Compute a 64-char hex SHA-256 memo key for a node execution context."""
	parts = [
		f"kind:{node_kind}",
		f"params:{_canonical_params(params)}",
		f"inputs:{json.dumps(sorted(str(a) for a in input_artifact_ids), separators=(',', ':'))}",
	]
	payload = "\n".join(parts).encode("utf-8")
	return hashlib.sha256(payload).hexdigest()


def compute_memo_key_for_node(
	node: Dict[str, Any],
	input_artifact_ids: Sequence[str],
) -> Optional[str]:
	"""
	Compute memo key for a runtime node payload.

	Returns None when node.data.meta.memoizable is explicitly False.
	"""
	data = node.get("data") if isinstance(node.get("data"), dict) else {}
	meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
	if meta.get("memoizable") is False:
		return None
	kind = str(data.get("kind") or "").strip()
	params = data.get("params") if isinstance(data.get("params"), dict) else {}
	return compute_memo_key(kind, params, input_artifact_ids)

