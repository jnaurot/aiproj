from __future__ import annotations

from typing import Any, Dict, Literal, Optional, Tuple

from pydantic import BaseModel, Field


SchemaState = Literal["fresh", "partial", "stale", "unknown"]
SchemaSource = Literal["sample", "artifact", "declared", "runtime", "component_contract", "unknown"]
SchemaTyped = Literal["table", "json", "text", "binary", "embeddings", "unknown"]
PortPlane = Literal["work", "param", "control"]
PortDirection = Literal["in", "out"]
PortCardinality = Literal["one", "many"]


class SchemaField(BaseModel):
	name: str
	type: SchemaTyped
	nativeType: Optional[str] = None
	nullable: bool = False

	model_config = {"extra": "ignore"}


class TypedSchema(BaseModel):
	type: SchemaTyped
	fields: list[SchemaField] = Field(default_factory=list)

	model_config = {"extra": "ignore"}


class SchemaObservation(BaseModel):
	typedSchema: Optional[TypedSchema] = None
	source: SchemaSource = "unknown"
	state: SchemaState = "unknown"
	schemaFingerprint: Optional[str] = None
	updatedAt: Optional[str] = None

	model_config = {"extra": "ignore"}


class SchemaEnvelope(BaseModel):
	inferredSchema: Optional[SchemaObservation] = None
	expectedInputSchemas: Optional[Dict[str, SchemaObservation]] = None
	expectedSchema: Optional[SchemaObservation] = None
	observedSchema: Optional[SchemaObservation] = None

	model_config = {"extra": "ignore"}


class PortDeclaration(BaseModel):
	plane: PortPlane = "work"
	affinity: PortPlane = "work"
	required: bool = False
	cardinality: PortCardinality = "many"
	behavior: Optional[Literal["once", "single_item", "batch"]] = None

	model_config = {"extra": "ignore"}


class PortDeclarationEnvelope(BaseModel):
	in_ports: Dict[str, PortDeclaration] = Field(default_factory=dict, alias="in")
	out_ports: Dict[str, PortDeclaration] = Field(default_factory=dict, alias="out")

	model_config = {"extra": "ignore", "populate_by_name": True}


def canonicalize_schema_envelope(raw: Any) -> Tuple[Optional[Dict[str, Any]], bool]:
	"""
	Best-effort canonicalization for node.data.schema payload.
	Returns (canonical_or_none, changed_flag).
	"""
	if raw is None:
		return None, False
	if not isinstance(raw, dict):
		return None, True
	model = SchemaEnvelope.model_validate(raw)
	dumped = model.model_dump(exclude_none=True)
	changed = dumped != raw
	return dumped, changed

