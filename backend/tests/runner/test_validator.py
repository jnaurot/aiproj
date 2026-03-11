"""
Unit tests for validator
"""
import pytest
from typing import Dict, Any
from app.runner.validator import GraphValidator, validate_node_connections, validate_parameters, validate_pipeline


class TestValidator:
    """Test validator functions"""
    
    def test_validate_empty_pipeline(self):
        """Test validating empty pipeline"""
        nodes = []
        edges = []
        
        validation_result = validate_pipeline(nodes, edges)
        
        assert validation_result["valid"]
        assert len(validation_result["errors"]) == 0
    
    def test_validate_simple_valid_pipeline(self):
        """Test validating simple valid pipeline"""
        nodes = [
            {
                "id": "source-1",
                "data": {"kind": "source"}
            },
            {
                "id": "transform-1",
                "data": {"kind": "transform"}
            },
            {
                "id": "llm-1",
                "data": {"kind": "llm"}
            }
        ]
        
        edges = [
            {"from": "source-1", "to": "transform-1"},
            {"from": "transform-1", "to": "llm-1"}
        ]
        
        validation_result = validate_pipeline(nodes, edges)
        
        assert validation_result["valid"]
        assert len(validation_result["errors"]) == 0
    
    def test_validate_nodes_without_edges(self):
        """Test nodes but no edges"""
        nodes = [
            {"id": "node-1", "data": {"kind": "source"}}
        ]
        
        validation_result = validate_pipeline(nodes, [])
        
        assert validation_result["valid"]
    
    def test_validate_node_with_invalid_kind(self):
        """Test node with invalid kind"""
        nodes = [
            {"id": "node-1", "data": {"kind": "invalid-kind"}}
        ]
        
        validation_result = validate_pipeline(nodes, [])
        
        assert not validation_result["valid"]
        assert len(validation_result["errors"]) > 0
    
    def test_validate_duplicate_node_ids(self):
        """Test detecting duplicate node ids"""
        nodes = [
            {"id": "node-1", "data": {"kind": "source"}},
            {"id": "node-1", "data": {"kind": "transform"}}  # Duplicate id
        ]
        
        validation_result = validate_pipeline(nodes, [])
        
        assert not validation_result["valid"]
        assert "duplicate" in validation_result["errors"][0].lower()
    
    def test_validate_node_missing_required_fields(self):
        """Test node missing required fields"""
        nodes = [
            {"id": "node-1"}  # Missing 'data' field
        ]
        
        validation_result = validate_pipeline(nodes, [])
        
        assert not validation_result["valid"]
    
    def test_validate_edge_with_nonexistent_source(self):
        """Test edge pointing to nonexistent source"""
        nodes = [{"id": "transform-1", "data": {"kind": "transform"}}]
        
        edges = [
            {"from": "nonexistent", "to": "transform-1"}
        ]
        
        validation_result = validate_pipeline(nodes, edges)
        
        assert not validation_result["valid"]
    
    def test_validate_edge_with_nonexistent_target(self):
        """Test edge pointing to nonexistent target"""
        nodes = [{"id": "source-1", "data": {"kind": "source"}}]
        
        edges = [
            {"from": "source-1", "to": "nonexistent"}
        ]
        
        validation_result = validate_pipeline(nodes, edges)
        
        assert not validation_result["valid"]
    
    def test_validate_multiple_source_nodes(self):
        """Test multiple source nodes in pipeline"""
        nodes = [
            {"id": "source-1", "data": {"kind": "source"}},
            {"id": "source-2", "data": {"kind": "source"}},
            {"id": "llm-1", "data": {"kind": "llm"}}
        ]
        
        edges = [
            {"from": "source-1", "to": "llm-1"},
            {"from": "source-2", "to": "llm-1"}
        ]
        
        validation_result = validate_pipeline(nodes, edges)
        
        assert validation_result["valid"]
    
    def test_validate_parallel_sources(self):
        """Test parallel sources feeding into different nodes"""
        nodes = [
            {"id": "source-1", "data": {"kind": "source"}},
            {"id": "source-2", "data": {"kind": "source"}},
            {"id": "llm-1", "data": {"kind": "llm"}},
            {"id": "llm-2", "data": {"kind": "llm"}}
        ]
        
        edges = [
            {"from": "source-1", "to": "llm-1"},
            {"from": "source-2", "to": "llm-2"}
        ]
        
        validation_result = validate_pipeline(nodes, edges)
        
        assert validation_result["valid"]
    
    def test_validate_source_with_target_transform(self):
        """Test source node with transform target"""
        nodes = [
            {"id": "source-1", "data": {"kind": "source"}},
            {"id": "transform-1", "data": {"kind": "transform"}},
            {"id": "llm-1", "data": {"kind": "llm"}}
        ]
        
        edges = [
            {"from": "source-1", "to": "transform-1"},
            {"from": "transform-1", "to": "llm-1"}
        ]
        
        validation_result = validate_pipeline(nodes, edges)
        
        assert validation_result["valid"]
    
    def test_validate_legacy_ports_are_ignored(self):
        """Legacy top-level ports field does not affect validation."""
        nodes = [
            {"id": "source-1", "data": {"kind": "source"}, "ports": []},
            {"id": "llm-1", "data": {"kind": "llm"}, "ports": []}
        ]
        
        edges = [
            {"from": "source-1", "to": "llm-1"}
        ]
        
        validation_result = validate_pipeline(nodes, edges)

        assert validation_result["valid"]


class TestEdgeValidation:
    """Test edge validation logic"""
    
    def test_validate_edge_valid(self):
        """Test valid edge validation"""
        result = validate_node_connections({"from": "node-1", "to": "node-2"})
        
        assert result["valid"]
    
    def test_validate_edge_self_connection(self):
        """Test edge connecting node to itself"""
        result = validate_node_connections({"from": "node-1", "to": "node-1"})
        
        assert not result["valid"]
        assert "self" in result["error"].lower()
    
    def test_validate_edge_missing_fields(self):
        """Test edge missing required fields"""
        result = validate_node_connections({"from": "node-1"})
        
        assert not result["valid"]


class TestParameterValidation:
    """Test parameter validation"""
    
    def test_validate_parameters_valid(self):
        """Test validating valid node parameters"""
        node = {
            "id": "node-1",
            "data": {
                "kind": "source",
                "params": {
                    "file_path": "/data.csv"
                }
            }
        }
        
        nodes_map = {n["id"]: n for n in [node]}
        
        validation_result = validate_parameters(node, nodes_map)
        
        assert validation_result["valid"]
    
    def test_validate_params_empty(self):
        """Test validating node with empty parameters"""
        node = {
            "id": "node-1",
            "data": {
                "kind": "source",
                "params": {}
            }
        }
        
        nodes_map = {n["id"]: n for n in [node]}
        
        validation_result = validate_parameters(node, nodes_map)
        
        # Should be valid even if empty
        assert validation_result["valid"]
    
    def test_validate_params_invalid_node(self):
        """Test validating with invalid node"""
        node = {
            "id": "node-1",
            "data": {}  # No kind
        }
        
        nodes_map = {n["id"]: n for n in [node]}
        
        validation_result = validate_parameters(node, nodes_map)
        
        assert not validation_result["valid"]


class TestPipelineComplexity:
    """Test pipeline complexity validation"""
    
    def test_validate_medium_complexity(self):
        """Test validating medium complexity pipeline"""
        nodes = [
            {"id": f"source-{i}", "data": {"kind": "source"}}
            for i in range(5)
        ] + [
            {"id": f"transform-{i}", "data": {"kind": "transform"}}
            for i in range(4)
        ] + [
            {"id": "llm-1", "data": {"kind": "llm"}}
        ]
        
        edges = []
        for i in range(4):
            edges.append({"from": f"source-{i}", "to": f"transform-{i}"})
            edges.append({"from": f"transform-{i}", "to": "llm-1"})
        
        validation_result = validate_pipeline(nodes, edges)
        
        assert validation_result["valid"]


class TestSchemaFirstEdgeValidation:
    def test_source_txt_to_llm_ignores_stale_inferred_table_type(self):
        validator = GraphValidator()
        graph = {
            "nodes": [
                {
                    "id": "src1",
                    "data": {
                        "kind": "source",
                        "sourceKind": "file",
                        "params": {"file_format": "txt"},
                        # stale inferred schema should not override declared/default source contract
                        "schema": {
                            "inferredSchema": {
                                "typedSchema": {
                                    "type": "table",
                                    "fields": [{"name": "text", "type": "text", "nullable": True}],
                                }
                            }
                        },
                    },
                },
                {
                    "id": "llm1",
                    "data": {"kind": "llm", "params": {"model": "dummy", "user_prompt": "summarize"}},
                },
            ],
            "edges": [{"id": "e1", "source": "src1", "target": "llm1"}],
        }

        result = validator.validate_pre_execution(graph)
        edge_errors = [e for e in result.errors if str(e.edge_id or "") == "e1"]
        assert all("TYPE_MISMATCH" not in str(e.code or "") for e in edge_errors)

    def test_validate_complex_chain(self):
        """Test validating complex chain of transforms"""
        chain_length = 10
        nodes = [{"id": f"node-{i}", "data": {"kind": "transform"}} for i in range(chain_length)]
        edges = [{"from": nodes[i]["id"], "to": nodes[i+1]["id"]} for i in range(chain_length-1)]
        
        validation_result = validate_pipeline(nodes, edges)
        
        assert validation_result["valid"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
