import importlib
import types

import pytest

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
	return NodeOutput(
		status='succeeded',
		metadata=None,
		execution_time_ms=1.0,
		data=[{'id': 1, 'text': 'a'}, {'id': 2, 'text': 'b'}],
	)


async def _run_graph(run_mod, graph: dict, tmp_path, run_id: str):
	events = []
	store = DiskArtifactStore(tmp_path / 'artifacts')
	cache = SqliteExecutionCache(str(tmp_path / 'artifacts' / 'meta' / 'artifacts.sqlite'))
	await run_mod.run_graph(
		run_id=run_id,
		graph=graph,
		run_from=None,
		bus=RunEventBus(run_id, on_emit=lambda e: events.append(dict(e))),
		artifact_store=store,
		cache=cache,
		graph_id=f'graph-{run_id}',
		runtime_ref=types.SimpleNamespace(get_global_cache_mode=lambda: 'force_off'),
	)
	return events


def _graph_with_sql(sql_params: dict) -> dict:
	return {
		'nodes': [
			{
				'id': 'source_1',
				'data': {
					'kind': 'source',
					'label': 'Source',
					'sourceKind': 'file',
					'params': {'file_path': 'dummy.json', 'file_format': 'json'},
				},
			},
			{
				'id': 'json_to_table_1',
				'data': {
					'kind': 'transform',
					'label': 'JsonToTable',
					'transformKind': 'json_to_table',
					'params': {'op': 'json_to_table', 'json_to_table': {'orient': 'records', 'rowsKey': 'rows'}},
				},
			},
			{
				'id': 'sql_1',
				'data': {
					'kind': 'transform',
					'label': 'SQL',
					'transformKind': 'sql',
					'params': {'op': 'sql', 'sql': sql_params},
				},
			},
		],
		'edges': [
			{'id': 'e1', 'source': 'source_1', 'target': 'json_to_table_1'},
			{'id': 'e2', 'source': 'json_to_table_1', 'target': 'sql_1'},
		],
	}


def _node_finished(events: list[dict], node_id: str) -> dict:
	items = [e for e in events if e.get('type') == 'node_finished' and e.get('nodeId') == node_id]
	assert items
	return items[-1]


@pytest.mark.asyncio
async def test_e2e_transform_sql_safe_mode_enabled_blocks_mutation_queries(monkeypatch, tmp_path):
	run_mod = importlib.import_module('app.runner.run')
	monkeypatch.setattr(run_mod, 'exec_source', _fake_exec_source)
	events = await _run_graph(
		run_mod,
		_graph_with_sql({'query': 'delete from input where id = 1', 'safe_mode': True}),
		tmp_path,
		'run-transform-sql-safe-mode-block',
	)
	finish = _node_finished(events, 'sql_1')
	assert finish.get('status') == 'failed'
	assert finish.get('errorCode') == 'TRANSFORM_SQL_SAFE_MODE_VIOLATION'


@pytest.mark.asyncio
async def test_e2e_transform_sql_within_limits_succeeds(monkeypatch, tmp_path):
	run_mod = importlib.import_module('app.runner.run')
	monkeypatch.setattr(run_mod, 'exec_source', _fake_exec_source)
	events = await _run_graph(
		run_mod,
		_graph_with_sql({'query': 'select id from input', 'safe_mode': True, 'max_output_rows': 10}),
		tmp_path,
		'run-transform-sql-within-limits',
	)
	finish = _node_finished(events, 'sql_1')
	assert finish.get('status') == 'succeeded'


@pytest.mark.asyncio
async def test_e2e_transform_sql_exceeding_limits_fails_with_machine_error(monkeypatch, tmp_path):
	run_mod = importlib.import_module('app.runner.run')
	monkeypatch.setattr(run_mod, 'exec_source', _fake_exec_source)
	events = await _run_graph(
		run_mod,
		_graph_with_sql({'query': 'select * from input', 'safe_mode': True, 'max_output_rows': 1}),
		tmp_path,
		'run-transform-sql-row-limit',
	)
	finish = _node_finished(events, 'sql_1')
	assert finish.get('status') == 'failed'
	assert finish.get('errorCode') == 'TRANSFORM_SQL_OUTPUT_ROW_LIMIT_EXCEEDED'
	details = finish.get('errorDetails') if isinstance(finish.get('errorDetails'), dict) else {}
	assert details.get('paramPath') == 'params.sql.max_output_rows'
	assert details.get('maxOutputRows') == 1
	assert details.get('actualRows') == 2
