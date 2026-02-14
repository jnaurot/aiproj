# """
# Unit tests for runner components
# """
# import pytest
# from unittest.mock import Mock, patch, MagicMock
# from datetime import datetime, timezone
# from app.runner.runner import GraphRunner
# from app.runner.metadata import ExecutionContext, FileMetadata, NodeOutput
# from app.runner.artifacts import ArtifactStore, RunBindings
# import pandas as pd


# class TestGraphRunner:
#     """Test GraphRunner"""
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_runner_initialization(self, mock_bus, mock_store):
#         """Test runner can be initialized"""
#         run_id = "run-123"
        
#         runner = GraphRunner(run_id=run_id)
        
#         assert runner.run_id == run_id
#         assert runner.bus is not None
#         assert runner.artifact_store is not None
    
#     def test_runner_with_custom_dependencies(self):
#         """Test runner with custom bus and store"""
#         custom_bus = Mock()
#         custom_store = Mock()
        
#         runner = GraphRunner(
#             run_id="run-456",
#             bus=custom_bus,
#             artifact_store=custom_store
#         )
        
#         assert runner.bus == custom_bus
#         assert runner.artifact_store == custom_store
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_add_node(self, mock_bus, mock_store):
#         """Test adding a node to the graph"""
#         runner = GraphRunner(run_id="run-1")
        
#         node = {
#             "id": "node-1",
#             "data": {
#                 "kind": "source",
#                 "sourceKind": "file"
#             },
#             "position": {"x": 0, "y": 0}
#         }
        
#         runner.add_node(node)
        
#         assert "node-1" in runner.nodes
#         assert runner.nodes["node-1"]["id"] == "node-1"
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_add_edge(self, mock_bus, mock_store):
#         """Test adding an edge between nodes"""
#         runner = GraphRunner(run_id="run-1")
        
#         runner.add_node({"id": "node-1", "data": {"kind": "source"}})
#         runner.add_node({"id": "node-2", "data": {"kind": "transform"}})
        
#         edge = {
#             "from": "node-1",
#             "to": "node-2",
#             "sourcePort": "output",
#             "targetPort": "input"
#         }
        
#         runner.add_edge(edge)
        
#         assert len(runner.edges) == 1
#         assert runner.edges[0]["from"] == "node-1"
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_remove_node(self, mock_bus, mock_store):
#         """Test removing a node"""
#         runner = GraphRunner(run_id="run-1")
        
#         runner.add_node({"id": "node-1", "data": {"kind": "source"}})
        
#         assert len(runner.nodes) == 1
        
#         runner.remove_node("node-1")
        
#         assert len(runner.nodes) == 0
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_validate_graph_empty(self, mock_bus, mock_store):
#         """Test validating empty graph"""
#         runner = GraphRunner(run_id="run-1")
        
#         errors = runner.validate_graph()
        
#         assert errors == []
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_validate_graph_valid(self, mock_bus, mock_store):
#         """Test validating valid graph"""
#         runner = GraphRunner(run_id="run-1")
        
#         runner.add_node({"id": "source", "data": {"kind": "source"}})
#         runner.add_node({"id": "transform", "data": {"kind": "transform"}})
#         runner.add_edge({"from": "source", "to": "transform"})
        
#         errors = runner.validate_graph()
        
#         assert errors == []
    
#     def test_execute_with_simple_graph(self):
#         """Test executing a simple graph"""
#         runner = GraphRunner(run_id="run-1")
        
#         mock_artifact = Mock(spec=ArtifactStore)
        
#         runner.nodes["source"] = {
#             "id": "source",
#             "data": {
#                 "kind": "source",
#                 "sourceKind": "file"
#             }
#         }
        
#         mock_artifact.create.return_value = "artifact-123"
        
#         with patch('app.executors.source.SourceExecutor') as mock_source_class:
#             mock_source = Mock()
#             mock_source.execute.return_value = pd.DataFrame({"name": ["Alice", "Bob"]})
#             mock_source_class.return_value = mock_source
            
#             result = runner.execute()
            
#             assert mock_source.execute.called
    
#     def test_execute_with_circular_dependency(self):
#         """Test that circular dependencies are detected"""
#         runner = GraphRunner(run_id="run-1")
        
#         runner.add_node({"id": "node-1", "data": {"kind": "source"}})
#         runner.add_node({"id": "node-2", "data": {"kind": "transform"}})
#         runner.add_node({"id": "node-3", "data": {"kind": "transform"}})
        
#         runner.add_edge({"from": "node-1", "to": "node-2"})
#         runner.add_edge({"from": "node-2", "to": "node-3"})
#         runner.add_edge({"from": "node-3", "to": "node-1"})
        
#         errors = runner.validate_graph()
        
#         assert len(errors) > 0


# class TestGraphRunnerNodes:
#     """Test node operations in runner"""
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_get_node(self, mock_bus, mock_store):
#         """Test getting a node by id"""
#         runner = GraphRunner(run_id="run-1")
        
#         node = {"id": "test-1", "data": {"kind": "source"}}
#         runner.add_node(node)
        
#         retrieved = runner.get_node("test-1")
        
#         assert retrieved == node
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_get_nonexistent_node(self, mock_bus, mock_store):
#         """Test getting nonexistent node returns None"""
#         runner = GraphRunner(run_id="run-1")
        
#         node = runner.get_node("nonexistent")
        
#         assert node is None
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_update_node(self, mock_bus, mock_store):
#         """Test updating a node"""
#         runner = GraphRunner(run_id="run-1")
        
#         node = {"id": "test-1", "data": {"kind": "source", "sourceKind": "file"}}
#         runner.add_node(node)
        
#         updated_data = {"kind": "source", "sourceKind": "database"}
#         runner.update_node("test-1", updated_data)
        
#         assert runner.nodes["test-1"]["data"] == updated_data
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_get_input_nodes(self, mock_bus, mock_store):
#         """Test getting input nodes for a given node"""
#         runner = GraphRunner(run_id="run-1")
        
#         runner.add_node({"id": "node-1", "data": {"kind": "source"}})
#         runner.add_node({"id": "node-2", "data": {"kind": "transform"}})
#         runner.add_node({"id": "node-3", "data": {"kind": "transform"}})
        
#         runner.add_edge({"from": "node-1", "to": "node-2"})
#         runner.add_edge({"from": "node-2", "to": "node-3"})
        
#         inputs = runner.get_input_nodes("node-2")
        
#         assert len(inputs) == 1
#         assert inputs[0] == "node-1"
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_get_output_nodes(self, mock_bus, mock_store):
#         """Test getting output nodes for a given node"""
#         runner = GraphRunner(run_id="run-1")
        
#         runner.add_node({"id": "node-1", "data": {"kind": "source"}})
#         runner.add_node({"id": "node-2", "data": {"kind": "transform"}})
#         runner.add_node({"id": "node-3", "data": {"kind": "transform"}})
        
#         runner.add_edge({"from": "node-1", "to": "node-2"})
#         runner.add_edge({"from": "node-2", "to": "node-3"})
        
#         outputs = runner.get_output_nodes("node-2")
        
#         assert len(outputs) == 1
#         assert outputs[0] == "node-3"


# class TestArtifactIntegration:
#     """Test artifact store integration"""
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_artifact_creation(self, mock_bus, mock_store):
#         """Test artifact creation during execution"""
#         from app.runner.artifacts import ArtifactStore as AS
        
#         mock_instance = Mock(spec=AS)
        
#         mock_instance.create.return_value = "artifact-123"
        
#         runner = GraphRunner(run_id="run-1", artifact_store=mock_instance)
        
#         with patch('app.executors.source.SourceExecutor') as mock_source_class:
#             mock_source = Mock()
#             mock_source_class.return_value = mock_source
            
#             result = runner.execute()
            
#             # Verify artifact was created
#             mock_instance.create.assert_called_once()
    
#     @patch('app.runner.runner.ArtifactStore')
#     def test_artifact_retrieval(self, mock_store):
#         """Test artifact retrieval"""
#         mock_instance = Mock(spec=ArtifactStore)
        
#         artifact_id = "art-456"
#         mock_instance.get.return_value = {"data": {"test": [1, 2]}}
#         mock_store.return_value = mock_instance
        
#         runner = GraphRunner(run_id="run-1", artifact_store=mock_instance)
        
#         result = runner.get_artifact(artifact_id)
        
#         assert result == {"data": {"test": [1, 2]}}


# class TestExecutionHistory:
#     """Test execution history tracking"""
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_execution_start_time(self, mock_bus, mock_store):
#         """Test execution start time is recorded"""
#         runner = GraphRunner(run_id="run-1")
        
#         assert runner.execution_start_time is None
        
#         runner.start_execution()
        
#         assert runner.execution_start_time is not None
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.runner.runner.RunEventBus')
#     def test_execution_end_time(self, mock_bus, mock_store):
#         """Test execution end time is recorded"""
#         runner = GraphRunner(run_id="run-1")
        
#         runner.start_execution()
#         runner.end_execution()
        
#         assert runner.execution_end_time is not None
    
#     @patch('app.runner.runner.ArtifactStore')
#     @patch('app.executors.source.SourceExecutor')
#     def test_get_execution_stats(self, mock_source_class, mock_bus, mock_store):
#         """Test getting execution statistics"""
#         runner = GraphRunner(run_id="run-1")
        
#         runner.start_execution()
        
#         runner.nodes["source"] = {"id": "source", "data": {"kind": "source"}}
        
#         mock_source = Mock()
#         mock_source_class.return_value = mock_source
        
#         runner.execute()
#         runner.end_execution()
        
#         stats = runner.get_execution_stats()
        
#         assert stats is not None
#         assert stats["nodes_executed"] == 1


# if __name__ == "__main__":
#     pytest.main([__file__, "-v"])
