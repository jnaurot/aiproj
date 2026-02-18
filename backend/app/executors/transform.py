from typing import Any, Dict, Optional
import pandas as pd
from pathlib import Path
from ..runner.events import RunEventBus
from ..runner.metadata import ExecutionContext, FileMetadata, NodeOutput
from datetime import datetime, timezone
from ..runner.artifacts import ArtifactStore


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def exec_transform(
    run_id: str,
    node: Dict[str, Any],
    context: ExecutionContext,
    input_metadata: Optional[FileMetadata],  # Added
    upstream_artifact_ids: Optional[list[str]] = None
) -> NodeOutput:
    """Execute transform node"""
    node_id = node.get("id", "<missing-node-id>")

    upstream_artifact_ids = upstream_artifact_ids or []

    # Validate context using assertions (matching tool.py pattern)
    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"
    assert hasattr(context, "artifact_store"), "context missing artifact_store"

    if not input_metadata:
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": "Transform node requires input data",
            "nodeId": node_id
        })
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=0.0,
            error="Transform node requires input data"
        )

    # Read input from artifact store
    try:
        # Handle storage_uri attribute (may not exist in all test cases)
        artifact_key = input_metadata.storage_uri.removeprefix("memory://") if hasattr(input_metadata, 'storage_uri') and input_metadata.storage_uri else f"artifact_{node_id}"

        input_bytes = await context.artifact_store.read(artifact_key)
        input_artifact = await context.artifact_store.get(artifact_key)
    except Exception as e:
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": f"Failed to read input artifact: {e}",
            "nodeId": node_id
        })
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=0.0,
            error=f"Failed to read input artifact: {str(e)}"
        )

    params = node["data"].get("params", {})
    transform_type = params.get("transform_type", "dataframe")
    file_type = input_metadata.file_type

    await context.bus.emit({
        "type": "log",
        "runId": run_id,
        "at": iso_now(),
        "level": "info",
        "message": f"Transform: {transform_type} on {file_type}",
        "nodeId": node_id
    })

    # --- DataFrame Operations ---
    if transform_type in ["dataframe", "dataframe-to-json", "json-to-dataframe"]:
        import hashlib

        # Read DataFrame from artifact bytes
        if "parquet" in file_type or "csv" in file_type or "excel" in file_type:
            # Write input bytes to temp file and read as DataFrame
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                tmp.write(input_bytes)
                tmp_path = tmp.name
            df = pd.read_parquet(tmp_path)
            Path(tmp_path).unlink()  # Clean up
        elif "json" in file_type:
            # Try to read as DataFrame
            try:
                # Write input bytes to temp file and read as DataFrame
                import tempfile
                with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
                    tmp.write(input_bytes)
                    tmp_path = tmp.name
                df = pd.read_json(tmp_path)
                Path(tmp_path).unlink()  # Clean up
            except Exception as e:
                print(f"[exec_transform] Failed to read JSON as DataFrame: {e}")
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": f"Failed to read JSON as DataFrame: {e}",
                    "nodeId": node_id
                })
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error=f"Failed to read JSON as DataFrame: {str(e)}"
                )
                # Fallback: single row DataFrame
                df = pd.DataFrame([pd.read_json(input_metadata.file_path)])
        else:
            print(f"[exec_transform] Unknown input format: {file_type}")
            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "error",
                "message": f"Input format not supported: {file_type}",
                "nodeId": node_id
            })
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=0.0,
                error=f"Input format not supported: {file_type}"
            )

        print(f"[exec_transform] DataFrame loaded: {len(df)} rows, {len(df.columns)} columns")

        # --- Dataframe→JSON Operation ---
        if transform_type == "dataframe-to-json":
            output_data = df.to_json(orient="records", default_handler=str)
            output_path = Path(f"/tmp/pipeline_data/{node_id}_output.json")
            output_path.parent.mkdir(exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(output_data)

            # Calculate hash
            hash_bytes = hashlib.sha256(output_data.encode()).hexdigest()

            output_metadata = FileMetadata(
                file_path=str(output_path),
                file_type="json",
                mime_type="application/json",
                size_bytes=len(output_data.encode("utf-8")),
                schema={"format": "json", "rows": len(df), "columns": list(df.columns)},
                row_count=len(df),
                access_method="local",
                content_hash=hash_bytes,
                node_id=node_id,
                params_hash=params_hash(params),
                estimated_memory_mb=input_metadata.size_bytes / (1024 * 1024) if input_metadata else 1.0
            )

            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": f"DataFrame→JSON: {len(df)} rows → {len(output_data)} bytes",
                "nodeId": node_id
            })

            return NodeOutput(
                status="succeeded",
                metadata=output_metadata,
                data=output_data,
                execution_time_ms=0.0
            )

        # --- JSON→DataFrame Operation ---
        elif transform_type == "json-to-dataframe":
            # JSON is already read as DataFrame above
            output_path = Path(f"/tmp/pipeline_data/{node_id}_output.parquet")
            output_path.parent.mkdir(exist_ok=True)
            df.to_parquet(output_path, index=False)

            # Calculate hash
            hash_bytes = hashlib.sha256(output_path.read_bytes()).hexdigest()

            output_metadata = FileMetadata(
                file_path=str(output_path),
                file_type="parquet",
                mime_type="application/vnd.apache.parquet",
                size_bytes=output_path.stat().st_size,
                schema={"format": "parquet", "rows": len(df), "columns": list(df.columns)},
                row_count=len(df),
                access_method="local",
                content_hash=hash_bytes,
                node_id=node_id,
                params_hash=params_hash(params),
                estimated_memory_mb=df.memory_usage(deep=True).sum() / (1024 * 1024)
            )

            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": f"JSON→DataFrame: {len(df)} rows",
                "nodeId": node_id
            })

            return NodeOutput(
                status="succeeded",
                metadata=output_metadata,
                execution_time_ms=0.0
            )

        # --- Regular DataFrame Transform ---
        result_df = df.copy()

        # Apply filter if specified
        if "filter" in params:
            filter_expr = params["filter"]
            try:
                result_df = result_df.query(filter_expr)
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "info",
                    "message": f"Applied filter: {filter_expr}",
                    "nodeId": node_id
                })
            except Exception as e:
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": f"Filter error: {e}",
                    "nodeId": node_id
                })
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error=f"Filter expression failed: {str(e)}"
                )

        # Apply map operation if specified
        if "function" in params:
            function_expr = params["function"]
            new_column_name = params.get("new_column_name", "mapped_value")
            try:
                result_df[new_column_name] = result_df.eval(function_expr)
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "info",
                    "message": f"Applied function: {function_expr}",
                    "nodeId": node_id
                })
            except Exception as e:
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": f"Function error: {e}",
                    "nodeId": node_id
                })
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error=f"Function expression failed: {str(e)}"
                )

        # Apply aggregation if specified
        if "aggregations" in params:
            aggregations = params["aggregations"]
            try:
                for column, agg_type in aggregations.items():
                    if agg_type == "sum":
                        result_df[column] = result_df[column].sum()
                    elif agg_type == "count":
                        result_df[column] = result_df[column].count()
                    elif agg_type == "mean":
                        result_df[column] = result_df[column].mean()
                    elif agg_type == "max":
                        result_df[column] = result_df[column].max()
                    elif agg_type == "min":
                        result_df[column] = result_df[column].min()
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "info",
                    "message": f"Applied aggregations: {aggregations}",
                    "nodeId": node_id
                })
            except Exception as e:
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": f"Aggregation error: {e}",
                    "nodeId": node_id
                })
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error=f"Aggregation failed: {str(e)}"
                )

        # Apply clean operation if specified
        if "clean" in params:
            clean_params = params["clean"]
            try:
                if clean_params.get("drop_na", False):
                    result_df = result_df.dropna()
                if clean_params.get("strip_whitespace", False):
                    for col in result_df.columns:
                        if result_df[col].dtype == "object":
                            result_df[col] = result_df[col].str.strip()
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "info",
                    "message": f"Applied cleaning: {clean_params}",
                    "nodeId": node_id
                })
            except Exception as e:
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": f"Cleaning error: {e}",
                    "nodeId": node_id
                })
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error=f"Cleaning failed: {str(e)}"
                )

        # Apply custom code if specified
        if "code" in params:
            code = params["code"]
            try:
                # Execute in isolated namespace with df and result_df
                exec_globals = {"df": df, "result_df": result_df}
                exec(code, exec_globals)
                # Update result_df from exec_globals
                result_df = exec_globals.get("result_df", result_df)
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "info",
                    "message": "Custom code executed successfully",
                    "nodeId": node_id
                })
            except Exception as e:
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": f"Custom code error: {e}",
                    "nodeId": node_id
                })
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=0.0,
                    error=f"Custom code execution failed: {str(e)}"
                )

        # Save result to output file
        output_path = Path(f"/tmp/pipeline_data/{node_id}_output.parquet")
        output_path.parent.mkdir(exist_ok=True)
        result_df.to_parquet(output_path)

        # Calculate hash
        hash_bytes = hashlib.sha256(output_path.read_bytes()).hexdigest()

        output_metadata = FileMetadata(
            file_path=str(output_path),
            file_type="parquet",
            mime_type="application/vnd.apache.parquet",
            size_bytes=output_path.stat().st_size,
            schema={"format": "parquet", "rows": len(result_df), "columns": list(result_df.columns)},
            row_count=len(result_df),
            access_method="local",
            content_hash=hash_bytes,
            node_id=node_id,
            params_hash=params_hash(params),
            estimated_memory_mb=result_df.memory_usage(deep=True).sum() / (1024 * 1024)
        )

        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "info",
            "message": f"Transform completed: {len(result_df)} rows",
            "nodeId": node_id
        })

        return NodeOutput(
            status="succeeded",
            metadata=output_metadata,
            execution_time_ms=0.0
        )

    else:
        error_msg = f"Unsupported transform type: {transform_type}"
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": error_msg,
            "nodeId": node_id
        })

        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=0.0,
            error=error_msg
        )


def params_hash(params: Dict[str, Any]) -> str:
    """Generate a hash for params dict"""
    import hashlib
    import json
    params_str = json.dumps(params, sort_keys=True)
    return hashlib.md5(params_str.encode()).hexdigest()
