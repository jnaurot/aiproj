from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app


def _component_payload(label: str):
    return {
        "graph": {
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
                        "ports": {"in": None, "out": "table"},
                        "params": {"source_type": "text", "text": "x", "output_mode": "rows"},
                    },
                }
            ],
            "edges": [],
        },
        "api": {
            "inputs": [
                {
                    "name": "in_data",
                    "portType": "table",
                    "required": True,
                    "typedSchema": {"type": "table", "fields": []},
                }
            ],
            "outputs": [
                {
                    "name": "out_data",
                    "portType": "json",
                    "required": True,
                    "typedSchema": {"type": "json", "fields": []},
                }
            ],
        },
        "configSchema": {},
    }


def _nested_component_payload(label: str):
    return {
        "graph": {
            "version": 1,
            "nodes": [
                {
                    "id": "cmp_child_node",
                    "type": "component",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "kind": "component",
                        "label": f"{label}-child",
                        "ports": {"in": None, "out": None},
                        "params": {
                            "componentRef": {
                                "componentId": "cmp_child",
                                "revisionId": "rev_child_1",
                                "apiVersion": "v1",
                            },
                            "api": {
                                "inputs": [],
                                "outputs": [
                                    {
                                        "name": "out_data",
                                        "portType": "json",
                                        "required": True,
                                        "typedSchema": {"type": "json", "fields": []},
                                    }
                                ],
                            },
                            "bindings": {"inputs": {}, "config": {}, "outputs": {}},
                            "config": {},
                        },
                    },
                }
            ],
            "edges": [],
        },
        "api": {
            "inputs": [],
            "outputs": [
                {
                    "name": "out_data",
                    "portType": "json",
                    "required": True,
                    "typedSchema": {"type": "json", "fields": []},
                }
            ],
        },
        "configSchema": {},
    }


def _component_with_single_dependency_payload(label: str, *, child_component_id: str, child_revision_id: str):
    return {
        "graph": {
            "version": 1,
            "nodes": [
                {
                    "id": "cmp_child_node",
                    "type": "component",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "kind": "component",
                        "label": f"{label}-child",
                        "ports": {"in": None, "out": None},
                        "params": {
                            "componentRef": {
                                "componentId": child_component_id,
                                "revisionId": child_revision_id,
                                "apiVersion": "v1",
                            },
                            "api": {
                                "inputs": [],
                                "outputs": [
                                    {
                                        "name": "out_data",
                                        "portType": "json",
                                        "required": True,
                                        "typedSchema": {"type": "json", "fields": []},
                                    }
                                ],
                            },
                            "bindings": {"inputs": {}, "config": {}, "outputs": {}},
                            "config": {},
                        },
                    },
                }
            ],
            "edges": [],
        },
        "api": {
            "inputs": [],
            "outputs": [
                {
                    "name": "out_data",
                    "portType": "json",
                    "required": True,
                    "typedSchema": {"type": "json", "fields": []},
                }
            ],
        },
        "configSchema": {},
    }


def test_component_routes_create_list_get():
    component_id = "cmp_route_test"
    with TestClient(app) as client:
        created = client.post(
            "/components",
            json={
                "componentId": component_id,
                "message": "seed",
                **_component_payload("v1"),
            },
        )
        assert created.status_code == 200, created.text
        body = created.json()
        assert body["componentId"] == component_id
        revision_id = body["revisionId"]

        listed_components = client.get("/components")
        assert listed_components.status_code == 200, listed_components.text
        components = listed_components.json()["components"]
        assert any(c["componentId"] == component_id for c in components)

        listed_revisions = client.get(f"/components/{component_id}/revisions")
        assert listed_revisions.status_code == 200, listed_revisions.text
        revisions = listed_revisions.json()["revisions"]
        assert len(revisions) >= 1
        assert revisions[0]["componentId"] == component_id

        fetched = client.get(f"/components/{component_id}/revisions/{revision_id}")
        assert fetched.status_code == 200, fetched.text
        detail = fetched.json()
        assert detail["componentId"] == component_id
        assert detail["revisionId"] == revision_id
        assert detail["definition"]["graph"]["nodes"][0]["data"]["label"] == "v1"
        assert isinstance(detail["definition"].get("contractSnapshot"), dict)
        assert detail["contractSnapshot"]["outputs"][0]["name"] == "out_data"


def test_component_routes_create_supports_nested_component_nodes():
    component_id = f"cmp_nested_route_{uuid4().hex[:8]}"
    child_component_id = f"cmp_nested_child_{uuid4().hex[:8]}"
    with TestClient(app) as client:
        child_create = client.post(
            "/components",
            json={
                "componentId": child_component_id,
                "message": "child-v1",
                **_component_payload("child-v1"),
            },
        )
        assert child_create.status_code == 200, child_create.text
        child_revision_id = str(child_create.json()["revisionId"] or "")
        assert child_revision_id

        res = client.post(
            "/components",
            json={
                "componentId": component_id,
                "message": "nested-v1",
                **_component_with_single_dependency_payload(
                    "nested-v1",
                    child_component_id=child_component_id,
                    child_revision_id=child_revision_id,
                ),
            },
        )
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["componentId"] == component_id
        revision_id = str(body["revisionId"] or "")
        assert revision_id

        fetched = client.get(f"/components/{component_id}/revisions/{revision_id}")
        assert fetched.status_code == 200, fetched.text
        definition = fetched.json()["definition"]
        nodes = definition["graph"]["nodes"]
        assert len(nodes) == 1
        assert str((nodes[0].get("data") or {}).get("kind") or "") == "component"


def test_component_routes_reject_dependency_reference_not_found():
    component_id = f"cmp_dep_missing_{uuid4().hex[:8]}"
    with TestClient(app) as client:
        res = client.post(
            "/components",
            json={
                "componentId": component_id,
                "message": "missing-dependency",
                **_component_with_single_dependency_payload(
                    "missing-dependency",
                    child_component_id="cmp_missing_child",
                    child_revision_id="crev_missing",
                ),
            },
        )
        assert res.status_code == 422, res.text
        detail = res.json().get("detail", {})
        diagnostics = detail.get("diagnostics") if isinstance(detail, dict) else []
        codes = {str(d.get("code") or "") for d in diagnostics if isinstance(d, dict)}
        assert "COMPONENT_DEPENDENCY_NOT_FOUND" in codes


def test_component_routes_dependency_manifest_collects_transitive_references():
    with TestClient(app) as client:
        grandchild_id = f"cmp_grandchild_{uuid4().hex[:8]}"
        grandchild_create = client.post(
            "/components",
            json={
                "componentId": grandchild_id,
                "message": "grandchild-v1",
                **_component_payload("grandchild-v1"),
            },
        )
        assert grandchild_create.status_code == 200, grandchild_create.text
        grandchild_rev = str(grandchild_create.json()["revisionId"] or "")
        assert grandchild_rev

        child_id = f"cmp_child_{uuid4().hex[:8]}"
        child_create = client.post(
            "/components",
            json={
                "componentId": child_id,
                "message": "child-v1",
                **_component_with_single_dependency_payload(
                    "child-v1",
                    child_component_id=grandchild_id,
                    child_revision_id=grandchild_rev,
                ),
            },
        )
        assert child_create.status_code == 200, child_create.text
        child_rev = str(child_create.json()["revisionId"] or "")
        assert child_rev

        parent_id = f"cmp_parent_{uuid4().hex[:8]}"
        parent_create = client.post(
            "/components",
            json={
                "componentId": parent_id,
                "message": "parent-v1",
                **_component_with_single_dependency_payload(
                    "parent-v1",
                    child_component_id=child_id,
                    child_revision_id=child_rev,
                ),
            },
        )
        assert parent_create.status_code == 200, parent_create.text
        parent_rev = str(parent_create.json()["revisionId"] or "")
        assert parent_rev

        parent_detail = client.get(f"/components/{parent_id}/revisions/{parent_rev}")
        assert parent_detail.status_code == 200, parent_detail.text
        definition = parent_detail.json()["definition"]
        manifest = definition.get("dependencyManifest") if isinstance(definition, dict) else {}
        deps = manifest.get("dependencies") if isinstance(manifest, dict) else []
        dep_keys = {
            (str(dep.get("componentId") or ""), str(dep.get("revisionId") or ""))
            for dep in deps
            if isinstance(dep, dict)
        }
        assert (child_id, child_rev) in dep_keys
        assert (grandchild_id, grandchild_rev) in dep_keys
        assert manifest.get("schemaVersion") == 1


def test_component_routes_reject_dependency_without_revision_id():
    component_id = f"cmp_dep_missing_rev_{uuid4().hex[:8]}"
    payload = _nested_component_payload("missing-rev")
    nodes = payload["graph"]["nodes"]
    nodes[0]["data"]["params"]["componentRef"]["revisionId"] = ""
    with TestClient(app) as client:
        res = client.post(
            "/components",
            json={
                "componentId": component_id,
                "message": "invalid-dependency-ref",
                **payload,
            },
        )
        assert res.status_code == 422, res.text
        detail = res.json().get("detail", {})
        diagnostics = detail.get("diagnostics") if isinstance(detail, dict) else []
        codes = {str(d.get("code") or "") for d in diagnostics if isinstance(d, dict)}
        assert "COMPONENT_DEPENDENCY_REFERENCE_INVALID" in codes


def test_component_routes_dependency_revision_override_allows_compatible_revision():
    with TestClient(app) as client:
        child_component_id = f"cmp_override_child_{uuid4().hex[:8]}"
        child_v1 = client.post(
            "/components",
            json={
                "componentId": child_component_id,
                "message": "child-v1",
                **_component_payload("child-v1"),
            },
        )
        assert child_v1.status_code == 200, child_v1.text
        child_v1_rev = str(child_v1.json()["revisionId"] or "")
        assert child_v1_rev

        child_v2 = client.post(
            "/components",
            json={
                "componentId": child_component_id,
                "message": "child-v2",
                **_component_payload("child-v2"),
            },
        )
        assert child_v2.status_code == 200, child_v2.text
        child_v2_rev = str(child_v2.json()["revisionId"] or "")
        assert child_v2_rev
        assert child_v2_rev != child_v1_rev

        parent_component_id = f"cmp_override_parent_{uuid4().hex[:8]}"
        parent_create = client.post(
            "/components",
            json={
                "componentId": parent_component_id,
                "message": "parent-v1",
                "dependencyRevisionOverrides": [
                    {
                        "componentId": child_component_id,
                        "fromRevisionId": child_v1_rev,
                        "toRevisionId": child_v2_rev,
                    }
                ],
                **_component_with_single_dependency_payload(
                    "parent-v1",
                    child_component_id=child_component_id,
                    child_revision_id=child_v1_rev,
                ),
            },
        )
        assert parent_create.status_code == 200, parent_create.text
        parent_rev = str(parent_create.json()["revisionId"] or "")
        assert parent_rev
        parent_detail = client.get(f"/components/{parent_component_id}/revisions/{parent_rev}")
        assert parent_detail.status_code == 200, parent_detail.text
        graph_nodes = parent_detail.json()["definition"]["graph"]["nodes"]
        component_ref = (((graph_nodes[0].get("data") or {}).get("params") or {}).get("componentRef") or {})
        assert str(component_ref.get("componentId") or "") == child_component_id
        assert str(component_ref.get("revisionId") or "") == child_v2_rev


def test_component_routes_dependency_revision_override_rejects_incompatible_revision():
    with TestClient(app) as client:
        child_component_id = f"cmp_override_bad_child_{uuid4().hex[:8]}"
        child_v1 = client.post(
            "/components",
            json={
                "componentId": child_component_id,
                "message": "child-v1",
                **_component_payload("child-v1"),
            },
        )
        assert child_v1.status_code == 200, child_v1.text
        child_v1_rev = str(child_v1.json()["revisionId"] or "")
        assert child_v1_rev

        incompatible_payload = _component_payload("child-v2-incompatible")
        incompatible_payload["api"]["outputs"][0]["portType"] = "text"
        incompatible_payload["api"]["outputs"][0]["typedSchema"] = {"type": "text", "fields": []}
        child_v2 = client.post(
            "/components",
            json={
                "componentId": child_component_id,
                "message": "child-v2",
                **incompatible_payload,
            },
        )
        assert child_v2.status_code == 200, child_v2.text
        child_v2_rev = str(child_v2.json()["revisionId"] or "")
        assert child_v2_rev
        assert child_v2_rev != child_v1_rev

        parent_component_id = f"cmp_override_bad_parent_{uuid4().hex[:8]}"
        parent_create = client.post(
            "/components",
            json={
                "componentId": parent_component_id,
                "message": "parent-v1",
                "dependencyRevisionOverrides": [
                    {
                        "componentId": child_component_id,
                        "fromRevisionId": child_v1_rev,
                        "toRevisionId": child_v2_rev,
                    }
                ],
                **_component_with_single_dependency_payload(
                    "parent-v1",
                    child_component_id=child_component_id,
                    child_revision_id=child_v1_rev,
                ),
            },
        )
        assert parent_create.status_code == 422, parent_create.text
        detail = parent_create.json().get("detail", {})
        diagnostics = detail.get("diagnostics") if isinstance(detail, dict) else []
        codes = {str(d.get("code") or "") for d in diagnostics if isinstance(d, dict)}
        assert "COMPONENT_DEPENDENCY_OVERRIDE_INCOMPATIBLE" in codes


def test_component_routes_reject_direct_dependency_cycle():
    component_id = f"cmp_cycle_direct_{uuid4().hex[:8]}"
    payload = _component_with_single_dependency_payload(
        "cycle-direct",
        child_component_id=component_id,
        child_revision_id="rev_any",
    )
    with TestClient(app) as client:
        res = client.post(
            "/components",
            json={
                "componentId": component_id,
                "message": "cycle-direct",
                **payload,
            },
        )
        assert res.status_code == 422, res.text
        detail = res.json().get("detail", {})
        diagnostics = detail.get("diagnostics") if isinstance(detail, dict) else []
        codes = {str(d.get("code") or "") for d in diagnostics if isinstance(d, dict)}
        assert "COMPONENT_DEPENDENCY_CYCLE" in codes


def test_component_routes_reject_transitive_dependency_cycle():
    with TestClient(app) as client:
        component_a = f"cmp_cycle_a_{uuid4().hex[:8]}"
        a_v1 = client.post(
            "/components",
            json={
                "componentId": component_a,
                "message": "a-v1",
                **_component_payload("a-v1"),
            },
        )
        assert a_v1.status_code == 200, a_v1.text
        a_v1_rev = str(a_v1.json()["revisionId"] or "")
        assert a_v1_rev

        component_c = f"cmp_cycle_c_{uuid4().hex[:8]}"
        c_v1 = client.post(
            "/components",
            json={
                "componentId": component_c,
                "message": "c-v1",
                **_component_with_single_dependency_payload(
                    "c-v1",
                    child_component_id=component_a,
                    child_revision_id=a_v1_rev,
                ),
            },
        )
        assert c_v1.status_code == 200, c_v1.text
        c_v1_rev = str(c_v1.json()["revisionId"] or "")
        assert c_v1_rev

        component_b = f"cmp_cycle_b_{uuid4().hex[:8]}"
        b_v1 = client.post(
            "/components",
            json={
                "componentId": component_b,
                "message": "b-v1",
                **_component_with_single_dependency_payload(
                    "b-v1",
                    child_component_id=component_c,
                    child_revision_id=c_v1_rev,
                ),
            },
        )
        assert b_v1.status_code == 200, b_v1.text
        b_v1_rev = str(b_v1.json()["revisionId"] or "")
        assert b_v1_rev

        a_v2 = client.post(
            "/components",
            json={
                "componentId": component_a,
                "message": "a-v2-cycle",
                **_component_with_single_dependency_payload(
                    "a-v2",
                    child_component_id=component_b,
                    child_revision_id=b_v1_rev,
                ),
            },
        )
        assert a_v2.status_code == 422, a_v2.text
        detail = a_v2.json().get("detail", {})
        diagnostics = detail.get("diagnostics") if isinstance(detail, dict) else []
        codes = {str(d.get("code") or "") for d in diagnostics if isinstance(d, dict)}
        assert "COMPONENT_DEPENDENCY_CYCLE" in codes


def test_component_routes_reject_invalid_payload():
    with TestClient(app) as client:
        res = client.post(
            "/components",
            json={
                "componentId": "cmp_invalid",
                "graph": {"version": 1, "nodes": []},
                "api": {"inputs": [], "outputs": []},
            },
        )
        assert res.status_code == 422, res.text


def test_component_routes_validate_endpoint():
    with TestClient(app) as client:
        ok = client.post(
            "/components/validate",
            json=_component_payload("validate-ok"),
        )
        assert ok.status_code == 200, ok.text
        body = ok.json()
        assert body["ok"] is True
        assert body["componentSchemaVersion"] == 1
        assert isinstance(body.get("normalizedDefinition"), dict)
        assert body["normalizedDefinition"]["api"]["outputs"][0]["typedSchema"]["type"] == "json"
        assert body["normalizedDefinition"]["api"]["outputs"][0]["typedSchema"]["fields"] == []

        canonicalized = client.post(
            "/components/validate",
            json={
                "graph": {"version": 1, "nodes": [], "edges": []},
                "api": {
                    "inputs": [],
                    "outputs": [
                        {
                            "name": "out_text",
                            "portType": "text",
                            "required": True,
                            "typedSchema": {
                                "type": "json",
                                "fields": [{"name": "x", "type": "text"}],
                            },
                        }
                    ],
                },
            },
        )
        assert canonicalized.status_code == 200, canonicalized.text
        canonicalized_body = canonicalized.json()
        assert canonicalized_body["ok"] is True
        normalized_output = canonicalized_body["normalizedDefinition"]["api"]["outputs"][0]
        assert normalized_output["typedSchema"]["type"] == "json"
        assert normalized_output["typedSchema"]["fields"] == [{"name": "x", "type": "text", "nullable": False}]

        bad = client.post(
            "/components/validate",
            json={
                "graph": {"version": 1, "nodes": [], "edges": []},
                "api": {
                    "inputs": [],
                    "outputs": [
                        {"name": "x", "portType": "bogus", "typedSchema": {"type": "oops", "fields": []}}
                    ],
                },
            },
        )
        assert bad.status_code == 200, bad.text
        bad_body = bad.json()
        assert bad_body["ok"] is False
        codes = {d.get("code") for d in bad_body.get("diagnostics", [])}
        assert "INVALID_PORT_TYPE" in codes
        assert "INVALID_TYPED_SCHEMA_TYPE" in codes


def test_component_validate_reports_builtin_environment_profiles(monkeypatch):
    from app.routes import components as mod

    monkeypatch.setattr(mod, "missing_packages_for_packages", lambda pkgs: ["numpy"] if "numpy" in pkgs else [])

    payload = {
        "graph": {
            "version": 1,
            "nodes": [
                {
                    "id": "tool_builtin",
                    "type": "tool",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "kind": "tool",
                        "label": "Tool Builtin",
                        "params": {
                            "provider": "builtin",
                            "builtin": {"toolId": "noop", "profileId": "core", "args": {}},
                        },
                        "ports": {"in": None, "out": "text"},
                    },
                }
            ],
            "edges": [],
        },
        "api": {
            "inputs": [],
            "outputs": [
                {
                    "name": "out_data",
                    "portType": "text",
                    "required": True,
                    "typedSchema": {"type": "text", "fields": []},
                }
            ],
        },
        "configSchema": {},
    }

    with TestClient(app) as client:
        res = client.post("/components/validate", json=payload)
        assert res.status_code == 200, res.text
        body = res.json()
        diagnostics = body.get("diagnostics") or []
        codes = {str(d.get("code") or "") for d in diagnostics if isinstance(d, dict)}
        assert "COMPONENT_ENV_PROFILES_REQUIRED" in codes
        assert "COMPONENT_ENV_PROFILES_MISSING" in codes


def test_component_validate_reports_missing_dependency_not_found():
    payload = _component_with_single_dependency_payload(
        "validate-missing-dependency",
        child_component_id="cmp_missing_validate",
        child_revision_id="crev_missing_validate",
    )
    with TestClient(app) as client:
        res = client.post("/components/validate", json=payload)
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["ok"] is False
        diagnostics = body.get("diagnostics") or []
        codes = {str(d.get("code") or "") for d in diagnostics if isinstance(d, dict)}
        assert "COMPONENT_DEPENDENCY_NOT_FOUND" in codes


def test_component_validate_reports_direct_cycle_when_component_id_provided():
    component_id = f"cmp_validate_cycle_{uuid4().hex[:8]}"
    payload = _component_with_single_dependency_payload(
        "validate-cycle",
        child_component_id=component_id,
        child_revision_id="rev_any",
    )
    with TestClient(app) as client:
        res = client.post(
            "/components/validate",
            json={"componentId": component_id, **payload},
        )
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["ok"] is False
        diagnostics = body.get("diagnostics") or []
        codes = {str(d.get("code") or "") for d in diagnostics if isinstance(d, dict)}
        assert "COMPONENT_DEPENDENCY_CYCLE" in codes


def test_component_routes_history_multiple_revisions():
    component_id = f"cmp_history_{uuid4().hex[:8]}"
    with TestClient(app) as client:
        created_v1 = client.post(
            "/components",
            json={
                "componentId": component_id,
                "message": "v1",
                **_component_payload("v1"),
            },
        )
        assert created_v1.status_code == 200, created_v1.text
        v1_id = created_v1.json()["revisionId"]

        created_v2 = client.post(
            "/components",
            json={
                "componentId": component_id,
                "message": "v2",
                **_component_payload("v2"),
            },
        )
        assert created_v2.status_code == 200, created_v2.text
        v2_id = created_v2.json()["revisionId"]

        created_v3 = client.post(
            "/components",
            json={
                "componentId": component_id,
                "message": "v3",
                **_component_payload("v3"),
            },
        )
        assert created_v3.status_code == 200, created_v3.text
        v3_id = created_v3.json()["revisionId"]

        listed_components = client.get("/components", params={"limit": 200, "offset": 0})
        assert listed_components.status_code == 200, listed_components.text
        components = listed_components.json()["components"]
        row = next((c for c in components if c["componentId"] == component_id), None)
        assert row is not None
        assert row["latestRevisionId"] == v3_id

        listed_revisions = client.get(f"/components/{component_id}/revisions", params={"limit": 10, "offset": 0})
        assert listed_revisions.status_code == 200, listed_revisions.text
        revisions = listed_revisions.json()["revisions"]
        assert [r["revisionId"] for r in revisions[:3]] == [v3_id, v2_id, v1_id]
        assert revisions[0]["parentRevisionId"] == v2_id
        assert revisions[1]["parentRevisionId"] == v1_id

        paged = client.get(f"/components/{component_id}/revisions", params={"limit": 1, "offset": 1})
        assert paged.status_code == 200, paged.text
        assert len(paged.json()["revisions"]) == 1
        assert paged.json()["revisions"][0]["revisionId"] == v2_id

        detail_v2 = client.get(f"/components/{component_id}/revisions/{v2_id}")
        assert detail_v2.status_code == 200, detail_v2.text
        assert detail_v2.json()["definition"]["graph"]["nodes"][0]["data"]["label"] == "v2"

