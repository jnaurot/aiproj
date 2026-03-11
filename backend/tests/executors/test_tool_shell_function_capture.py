import sys
import types
import shutil
from datetime import datetime, timezone

import pytest

from app.executors.tool import exec_tool
from app.runner.artifacts import Artifact, MemoryArtifactStore, RunBindings
from app.runner.events import RunEventBus
from app.runner.metadata import ExecutionContext


def _context(run_id: str) -> ExecutionContext:
    return ExecutionContext(
        run_id=run_id,
        bus=RunEventBus(run_id),
        artifact_store=MemoryArtifactStore(),
        bindings=RunBindings(run_id),
        graph_id=f"graph_{run_id}",
    )


def _duckdb_or_skip():
    duckdb = pytest.importorskip("duckdb")
    if not callable(getattr(duckdb, "connect", None)):
        pytest.skip("duckdb runtime not available (stubbed module present)")
    return duckdb


@pytest.mark.asyncio
async def test_shell_capture_success_payload_is_structured():
    ctx = _context("shell-ok")
    node = {
        "id": "tool_shell",
        "data": {
            "params": {
                "provider": "shell",
                "permissions": {"subprocess": True},
                "shell": {"command": "python -c \"print('hello')\""},
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id="shell-ok", node=node, context=ctx, upstream_artifact_ids=[])
    assert out.status == "succeeded"
    assert isinstance(out.data, dict)
    assert out.data.get("kind") == "json"
    payload = out.data.get("payload") or {}
    assert payload.get("exit_code") == 0
    assert payload.get("ok") is True
    assert "hello" in str(payload.get("stdout") or "")


@pytest.mark.asyncio
async def test_shell_nonzero_can_be_non_fatal_when_configured():
    ctx = _context("shell-nonzero")
    node = {
        "id": "tool_shell",
        "data": {
            "params": {
                "provider": "shell",
                "permissions": {"subprocess": True},
                "shell": {
                    "command": "python -c \"import sys; sys.exit(7)\"",
                    "fail_on_nonzero": False,
                },
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id="shell-nonzero", node=node, context=ctx, upstream_artifact_ids=[])
    assert out.status == "succeeded"
    payload = (out.data or {}).get("payload") or {}
    assert payload.get("exit_code") == 7
    assert payload.get("ok") is False


@pytest.mark.asyncio
async def test_function_capture_wraps_result():
    mod = types.ModuleType("test_tool_mod")

    def run(call_input, call_args):
        return {
            "seen_input_type": type(call_input).__name__,
            "seen_args": call_args,
            "ok": True,
        }

    mod.run = run
    sys.modules["test_tool_mod"] = mod

    ctx = _context("function-ok")
    node = {
        "id": "tool_function",
        "data": {
            "params": {
                "provider": "function",
                "function": {
                    "module": "test_tool_mod",
                    "export": "run",
                    "args": {"x": 1},
                    "capture_output": True,
                },
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id="function-ok", node=node, context=ctx, upstream_artifact_ids=[])
    assert out.status == "succeeded"
    payload = (out.data or {}).get("payload") or {}
    assert payload.get("ok") is True
    assert payload.get("module") == "test_tool_mod"
    assert payload.get("export") == "run"
    assert isinstance(payload.get("result"), dict)


@pytest.mark.asyncio
async def test_shell_falls_back_when_asyncio_subprocess_not_supported(monkeypatch):
    async def _raise_not_impl(*args, **kwargs):
        raise NotImplementedError()

    monkeypatch.setattr("app.executors.tool.asyncio.create_subprocess_shell", _raise_not_impl)

    ctx = _context("shell-fallback")
    node = {
        "id": "tool_shell",
        "data": {
            "params": {
                "provider": "shell",
                "permissions": {"subprocess": True},
                "shell": {"command": "python -c \"print('fallback-ok')\""},
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id="shell-fallback", node=node, context=ctx, upstream_artifact_ids=[])
    assert out.status == "succeeded"
    payload = (out.data or {}).get("payload") or {}
    assert payload.get("ok") is True
    assert "fallback-ok" in str(payload.get("stdout") or "")


@pytest.mark.asyncio
async def test_js_capture_wraps_result_and_args():
    if not shutil.which("node"):
        pytest.skip("Node.js runtime not available in PATH")

    ctx = _context("js-ok")
    node = {
        "id": "tool_js",
        "data": {
            "params": {
                "provider": "js",
                "permissions": {"subprocess": True},
                "js": {
                    "code": "result = { sum: args.a + args.b };",
                    "args": {"a": 2, "b": 5},
                    "capture_output": True,
                },
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id="js-ok", node=node, context=ctx, upstream_artifact_ids=[])
    assert out.status == "succeeded"
    payload = (out.data or {}).get("payload") or {}
    assert payload.get("ok") is True
    assert payload.get("args") == {"a": 2, "b": 5}
    assert (payload.get("result") or {}).get("sum") == 7


@pytest.mark.asyncio
async def test_python_capture_wraps_result_and_args():
    ctx = _context("python-ok")
    node = {
        "id": "tool_python",
        "data": {
            "params": {
                "provider": "python",
                "python": {
                    "code": "result = {'sum': args.get('a', 0) + args.get('b', 0)}",
                    "args": {"a": 4, "b": 6},
                    "capture_output": True,
                },
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id="python-ok", node=node, context=ctx, upstream_artifact_ids=[])
    assert out.status == "succeeded"
    payload = (out.data or {}).get("payload") or {}
    assert payload.get("ok") is True
    assert payload.get("args") == {"a": 4, "b": 6}
    assert (payload.get("result") or {}).get("sum") == 10


@pytest.mark.asyncio
async def test_db_capture_wraps_rows_and_params(tmp_path):
    duckdb = _duckdb_or_skip()
    db_path = tmp_path / "db_tool_test.duckdb"
    conn = duckdb.connect(database=str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("create table items(id integer primary key, name text)")
        cur.execute("insert into items(id, name) values (?, ?)", (1, "alpha"))
        cur.execute("insert into items(id, name) values (?, ?)", (2, "beta"))
        conn.commit()
    finally:
        conn.close()

    ctx = _context("db-ok")
    node = {
        "id": "tool_db",
        "data": {
            "params": {
                "provider": "db",
                "db": {
                    "connectionRef": f"duckdb:///{db_path}",
                    "sql": "select id, name from items where id >= 1 order by id",
                    "params": {},
                    "capture_output": True,
                },
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id="db-ok", node=node, context=ctx, upstream_artifact_ids=[])
    assert out.status == "succeeded"
    payload = (out.data or {}).get("payload") or {}
    assert payload.get("ok") is True
    assert payload.get("params") == {}
    result = payload.get("result") or {}
    assert result.get("row_count") == 2
    assert isinstance(result.get("rows"), list)
    assert len(result.get("rows")) == 2


@pytest.mark.asyncio
async def test_db_can_create_table_from_upstream_input(tmp_path):
    duckdb = _duckdb_or_skip()
    db_path = tmp_path / "db_tool_upstream.duckdb"
    csv_bytes = (
        b"id,sku,category,qty,price,region\n"
        b"1,A100,alpha,2,10.0,E\n"
        b"2,A200,alpha,1,20.0,W\n"
        b"3,B100,beta,5,5.0,E\n"
    )
    upstream_artifact_id = "art_source_inventory"

    ctx = _context("db-upstream")
    await ctx.artifact_store.write(
        Artifact(
            artifact_id=upstream_artifact_id,
            node_kind="source",
            params_hash="source-hash",
            upstream_ids=[],
            created_at=datetime.now(timezone.utc),
            execution_version="v1",
            mime_type="text/csv; charset=utf-8",
            payload_type="table",
            size_bytes=len(csv_bytes),
            storage_uri=f"memory://{upstream_artifact_id}",
        ),
        csv_bytes,
    )

    create_node = {
        "id": "tool_db_create",
        "data": {
            "params": {
                "provider": "db",
                "db": {
                    "connectionRef": f"duckdb:///{db_path}",
                    "sql": "create table inventory as select * from input",
                    "params": {},
                    "capture_output": True,
                },
                "output": {"mode": "json"},
            }
        },
    }
    create_out = await exec_tool(
        run_id="db-upstream",
        node=create_node,
        context=ctx,
        upstream_artifact_ids=[upstream_artifact_id],
    )
    assert create_out.status == "succeeded"

    verify_node = {
        "id": "tool_db_verify",
        "data": {
            "params": {
                "provider": "db",
                "db": {
                    "connectionRef": f"duckdb:///{db_path}",
                    "sql": "select count(*) as n from inventory",
                    "params": {},
                    "capture_output": True,
                },
                "output": {"mode": "json"},
            }
        },
    }
    verify_out = await exec_tool(run_id="db-upstream", node=verify_node, context=ctx, upstream_artifact_ids=[])
    assert verify_out.status == "succeeded"
    payload = (verify_out.data or {}).get("payload") or {}
    result = payload.get("result") or {}
    rows = result.get("rows") or []
    assert len(rows) == 1
    assert int(rows[0].get("n")) == 3


@pytest.mark.asyncio
async def test_db_dedupes_duplicate_upstream_artifact_ids():
    _duckdb_or_skip()
    csv_bytes = (
        b"id,sku,category,qty,price,region\n"
        b"1,A100,alpha,2,10.0,E\n"
        b"2,A200,alpha,1,20.0,W\n"
    )
    aid = "art_source_dupe"
    ctx = _context("db-dupe")
    await ctx.artifact_store.write(
        Artifact(
            artifact_id=aid,
            node_kind="source",
            params_hash="source-hash",
            upstream_ids=[],
            created_at=datetime.now(timezone.utc),
            execution_version="v1",
            mime_type="text/csv; charset=utf-8",
            payload_type="table",
            size_bytes=len(csv_bytes),
            storage_uri=f"memory://{aid}",
        ),
        csv_bytes,
    )

    node = {
        "id": "tool_db_dupe",
        "data": {
            "params": {
                "provider": "db",
                "db": {
                    "connectionRef": ":memory:",
                    "sql": "select count(*) as n from input",
                    "params": {},
                    "capture_output": True,
                },
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id="db-dupe", node=node, context=ctx, upstream_artifact_ids=[aid, aid])
    assert out.status == "succeeded"
    payload = (out.data or {}).get("payload") or {}
    result = payload.get("result") or {}
    input_tables = result.get("inputTables") or []
    assert len(input_tables) == 1
    rows = result.get("rows") or []
    assert len(rows) == 1
    assert int(rows[0].get("n")) == 2


@pytest.mark.asyncio
async def test_db_materializes_table_port_from_plain_text_csv():
    _duckdb_or_skip()
    csv_text = (
        "id,sku,category,qty,price,region\n"
        "1,A100,alpha,2,10.0,E\n"
        "2,A200,alpha,1,20.0,W\n"
        "3,B100,beta,5,5.0,E\n"
    )
    aid = "art_source_plain_table"
    ctx = _context("db-plain-table")
    raw = csv_text.encode("utf-8")
    await ctx.artifact_store.write(
        Artifact(
            artifact_id=aid,
            node_kind="source",
            params_hash="source-hash",
            upstream_ids=[],
            created_at=datetime.now(timezone.utc),
            execution_version="v1",
            mime_type="text/plain; charset=utf-8",
            payload_type="table",
            size_bytes=len(raw),
            storage_uri=f"memory://{aid}",
        ),
        raw,
    )

    node = {
        "id": "tool_db_plain_table",
        "data": {
            "params": {
                "provider": "db",
                "db": {
                    "connectionRef": ":memory:",
                    "sql": "select count(*) as n from input",
                    "params": {},
                    "capture_output": True,
                },
                "output": {"mode": "json"},
            }
        },
    }

    out = await exec_tool(run_id="db-plain-table", node=node, context=ctx, upstream_artifact_ids=[aid])
    assert out.status == "succeeded"
    payload = (out.data or {}).get("payload") or {}
    result = payload.get("result") or {}
    rows = result.get("rows") or []
    assert len(rows) == 1
    assert int(rows[0].get("n")) == 4

