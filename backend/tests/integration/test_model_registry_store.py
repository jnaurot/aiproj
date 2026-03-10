from pathlib import Path

import pytest

from app.model_registry import ModelRegistryStore


def test_model_registry_store_register_and_promote(tmp_path: Path):
	db = tmp_path / "models.sqlite"
	store = ModelRegistryStore(str(db))

	v1 = store.register_version(
		model_id="model_iris",
		model_name="Iris Classifier",
		run_id="run-a",
		graph_id="graph-a",
		artifact_id="artifact-a",
		metrics={"flat": {"score": 0.91}},
		provenance={"datasetVersionId": "ds-v1"},
	)
	assert v1.version_number == 1
	assert v1.stage == "candidate"

	v2 = store.register_version(
		model_id="model_iris",
		model_name="Iris Classifier",
		run_id="run-b",
		graph_id="graph-a",
		artifact_id="artifact-b",
		metrics={"flat": {"score": 0.95}},
		provenance={"datasetVersionId": "ds-v2"},
	)
	assert v2.version_number == 2
	assert v2.stage == "candidate"

	promoted_baseline = store.promote_version(
		model_id="model_iris",
		version_id=v1.version_id,
		to_stage="baseline",
		promoted_by="alice",
	)
	assert promoted_baseline["ok"] is True
	assert promoted_baseline["changed"] is True
	assert promoted_baseline["toStage"] == "baseline"

	with pytest.raises(RuntimeError):
		store.promote_version(
			model_id="model_iris",
			version_id=v2.version_id,
			to_stage="baseline",
			force=False,
		)

	forced = store.promote_version(
		model_id="model_iris",
		version_id=v2.version_id,
		to_stage="baseline",
		force=True,
		promoted_by="bob",
	)
	assert forced["ok"] is True
	assert forced["demotedVersionId"] == v1.version_id

	v1_row = store.get_version("model_iris", v1.version_id)
	assert v1_row is not None
	assert v1_row["stage"] == "candidate"
	v2_row = store.get_version("model_iris", v2.version_id)
	assert v2_row is not None
	assert v2_row["stage"] == "baseline"
	assert v2_row["promotedBy"] == "bob"


def test_model_registry_store_rejects_invalid_transition(tmp_path: Path):
	db = tmp_path / "models.sqlite"
	store = ModelRegistryStore(str(db))
	v1 = store.register_version(model_id="model_x", model_name="Model X")
	with pytest.raises(ValueError):
		store.promote_version(model_id="model_x", version_id=v1.version_id, to_stage="prod")
