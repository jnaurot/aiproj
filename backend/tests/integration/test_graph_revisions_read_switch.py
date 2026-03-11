from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def _graph_payload(label: str):
	return {
		"version": 1,
		"nodes": [
			{
				"id": "n1",
				"type": "source",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "source",
					"label": label,
					"sourceKind": "file",
					"status": "idle",
					"params": {"source_type": "text", "text": "x", "output_mode": "rows"},
				},
			}
		],
		"edges": [],
	}


def _graph_with_builtin_tool(profile_id: str = "core"):
	return {
		"version": 1,
		"nodes": [
			{
				"id": "tool_builtin_1",
				"type": "tool",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "tool",
					"label": "Tool Builtin",
					"params": {
						"provider": "builtin",
						"builtin": {
							"toolId": "noop",
							"profileId": profile_id,
							"args": {},
						},
					},
				},
			}
		],
		"edges": [],
	}


def test_graph_read_path_always_enabled(monkeypatch):
	graph_id = "graph_phase3_read_switch"

	# seed revision directly in store
	with TestClient(app) as client:
		client.app.state.graph_revisions.create_revision(
			graph_id=graph_id,
			graph=_graph_payload("seeded"),
			message="seed",
		)

		monkeypatch.setenv("GRAPH_STORE_V2_READ", "0")
		on = client.get(f"/graphs/{graph_id}/latest")
		assert on.status_code == 200, on.text
		body = on.json()
		assert body["graphId"] == graph_id
		assert body["graph"]["nodes"][0]["data"]["label"] == "seeded"


def test_graph_feature_flags_runtime_update():
	with TestClient(app) as client:
		before = client.get("/graphs/feature-flags")
		assert before.status_code == 200, before.text
		before_body = before.json()
		assert before_body["flags"]["GRAPH_STORE_V2_READ"] is True
		assert before_body["flags"]["GRAPH_STORE_V2_WRITE"] is True
		assert before_body["flags"]["GRAPH_EXPORT_V2"] is True

		res = client.put(
			"/graphs/feature-flags",
			json={
				"GRAPH_STORE_V2_READ": True,
				"GRAPH_STORE_V2_WRITE": True,
				"GRAPH_EXPORT_V2": False,
			},
		)
		assert res.status_code == 410, res.text


def test_graph_export_import_package_v2():
	graph_id = "graph_phase5_pkg"
	with TestClient(app) as client:
		created = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "seed",
				"graph": _graph_payload("phase5"),
			},
		)
		assert created.status_code == 200, created.text

		exported = client.get(f"/graphs/{graph_id}/export")
		assert exported.status_code == 200, exported.text
		pkg = exported.json()["package"]
		assert pkg["manifest"]["packageType"] == "aipgraph"
		assert int(pkg["manifest"]["packageVersion"]) == 2
		env_deps = (((pkg.get("manifest") or {}).get("dependencies") or {}).get("environmentProfiles") or [])
		assert isinstance(env_deps, list)

		imported = client.post(
			"/graphs/import",
			json={
				"package": pkg,
				"targetGraphId": f"{graph_id}_imported",
				"message": "imported",
			},
		)
		assert imported.status_code == 200, imported.text
		body = imported.json()
		assert body["graphId"] == f"{graph_id}_imported"
		assert body["migrationReport"]["format"] == "aipgraph_v2"

		legacy = client.post(
			"/graphs/import",
			json={
				"package": {
					"version": 1,
					"nodes": [],
					"edges": [],
				},
				"targetGraphId": f"{graph_id}_legacy",
			},
		)
		assert legacy.status_code == 200, legacy.text
		legacy_report = (legacy.json() or {}).get("migrationReport") or {}
		assert legacy_report.get("format") == "raw_graph_legacy"


def test_graph_export_import_reports_component_dependencies():
	graph_id = "graph_phase5_pkg_components"
	with TestClient(app) as client:
		created_component = client.post(
			"/components",
			json={
				"componentId": "cmp_dep_test",
				"message": "seed",
				"graph": {"version": 1, "nodes": [], "edges": []},
				"api": {"inputs": [], "outputs": []},
				"configSchema": {},
			},
		)
		assert created_component.status_code == 200, created_component.text
		crev = created_component.json()["revisionId"]

		created_graph = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "seed-components",
				"graph": {
					"version": 1,
					"nodes": [
						{
							"id": "cmp_1",
							"type": "component",
							"position": {"x": 0, "y": 0},
							"data": {
								"kind": "component",
								"label": "Component",
								"params": {
									"componentRef": {
										"componentId": "cmp_dep_test",
										"revisionId": crev,
										"apiVersion": "v1",
									},
									"bindings": {"inputs": {}, "config": {}},
									"config": {},
									"api": {"inputs": [], "outputs": []},
								},
							},
						}
					],
					"edges": [],
				},
			},
		)
		assert created_graph.status_code == 200, created_graph.text

		exported = client.get(f"/graphs/{graph_id}/export")
		assert exported.status_code == 200, exported.text
		pkg = exported.json()["package"]
		component_deps = (((pkg.get("manifest") or {}).get("dependencies") or {}).get("components") or [])
		assert len(component_deps) == 1
		assert component_deps[0]["componentId"] == "cmp_dep_test"
		assert component_deps[0]["revisionId"] == crev

		# Force unresolved deps report by mutating package before import.
		pkg["manifest"]["dependencies"]["components"].append(
			{"componentId": "cmp_missing", "revisionId": "crev_missing", "apiVersion": "v1"}
		)
		imported = client.post(
			"/graphs/import",
			json={
				"package": pkg,
				"targetGraphId": f"{graph_id}_imported",
				"message": "imported-components",
			},
		)
		assert imported.status_code == 200, imported.text
		report = imported.json()["migrationReport"]
		assert isinstance(report.get("componentDependencies"), list)
		assert any(d.get("componentId") == "cmp_dep_test" for d in report.get("componentDependencies", []))
		unresolved = report.get("unresolvedComponentDependencies") or []
		assert any(d.get("componentId") == "cmp_missing" for d in unresolved)


def test_graph_export_import_reports_environment_profile_dependencies(monkeypatch):
	from app.routes import graphs as mod

	graph_id = "graph_phase5_pkg_env_profiles"
	monkeypatch.setattr(mod, "missing_packages_for_packages", lambda pkgs: ["numpy"] if "numpy" in pkgs else [])
	with TestClient(app) as client:
		created_graph = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "seed-env-profiles",
				"graph": _graph_with_builtin_tool("core"),
			},
		)
		assert created_graph.status_code == 200, created_graph.text

		exported = client.get(f"/graphs/{graph_id}/export")
		assert exported.status_code == 200, exported.text
		pkg = exported.json()["package"]
		env_deps = (((pkg.get("manifest") or {}).get("dependencies") or {}).get("environmentProfiles") or [])
		assert any(d.get("profileId") == "core" for d in env_deps)

		imported = client.post(
			"/graphs/import",
			json={
				"package": pkg,
				"targetGraphId": f"{graph_id}_imported",
				"message": "imported-env-profiles",
			},
		)
		assert imported.status_code == 200, imported.text
		report = imported.json()["migrationReport"]
		assert any(d.get("profileId") == "core" for d in (report.get("environmentProfiles") or []))
		assert any(d.get("profileId") == "core" for d in (report.get("missingEnvironmentProfiles") or []))
		assert any("environment profiles are not installed" in str(w).lower() for w in (report.get("warnings") or []))
