# backend/app/runner/validator.py
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass
from .schemas import validate_node_params  # Import schema validation

from pprint import pformat

@dataclass
class ValidationError:
    code: str
    message: str
    node_id: Optional[str] = None
    edge_id: Optional[str] = None

@dataclass
class ValidationResult:
    valid: bool
    errors: List[ValidationError]
    warnings: List[ValidationError]

class GraphValidator:
    """Pre-execution and runtime validation"""
    def validate_pre_execution(self, graph: Dict[str, Any]) -> ValidationResult:
        """Comprehensive validation before execution starts"""
        errors = []
        warnings = []
        
        # 1. Structural validation
        errors.extend(self._check_cycles(graph))
        errors.extend(self._check_orphaned_nodes(graph))
        
        # 2. Type validation
        errors.extend(self._validate_port_types(graph))
        
        # 3. Schema validation
        errors.extend(self._validate_node_params_schema(graph))
        
        # 4. Resource validation
        warnings.extend(self._check_resource_availability(graph))
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def _validate_node_params_schema(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Validate using Pydantic schemas"""
        errors = []
        nodes = graph.get("nodes", [])
        
        for node in nodes:
            node_id = node["id"]
            node_label = node["data"].get("label", node_id)
            
            
            #DEBUGGING LOGS START
            if node.get("data", {}).get("kind") == "llm":
                print("\n[VALIDATOR] LLM NODE RAW:")
                print(pformat({
                    "id": node_id,
                    "llmKind": node.get("data", {}).get("llmKind"),
                    "params": node.get("data", {}).get("params"),
                })[:8000])
            #DEBUGGING LOGS END
            
            # Use schema validation
            param_errors = validate_node_params(node)
            
            for error_msg in param_errors:
                errors.append(ValidationError(
                    code="INVALID_PARAMS",
                    message=f"Node '{node_label}': {error_msg}",
                    node_id=node_id
                ))
        
        return errors
    
    def _check_cycles(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Detect cycles using DFS"""
        errors = []
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])
        
        if not nodes:
            return errors
        
        # Build adjacency list
        adj: Dict[str, List[str]] = {n["id"]: [] for n in nodes}
        for e in edges:
            source = e.get("source")
            target = e.get("target")
            if source in adj and target in adj:
                adj[source].append(target)
        
        # DFS cycle detection
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n["id"]: WHITE for n in nodes}
        
        def has_cycle(node_id: str, path: List[str]) -> bool:
            if color[node_id] == GRAY:
                # Found a back edge - cycle detected
                cycle_start = path.index(node_id)
                cycle_nodes = " -> ".join(path[cycle_start:] + [node_id])
                errors.append(ValidationError(
                    code="CYCLE_DETECTED",
                    message=f"Cycle detected: {cycle_nodes}"
                ))
                return True
            
            if color[node_id] == BLACK:
                return False
            
            color[node_id] = GRAY
            path.append(node_id)
            
            for neighbor in adj.get(node_id, []):
                if has_cycle(neighbor, path):
                    return True
            
            path.pop()
            color[node_id] = BLACK
            return False
        
        for node in nodes:
            if color[node["id"]] == WHITE:
                has_cycle(node["id"], [])
        
        return errors
    
    def _check_orphaned_nodes(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Check for nodes with no connections"""
        errors = []
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])
        
        if len(nodes) <= 1:
            return errors  # Single node or empty graph is fine
        
        connected_nodes = set()
        for e in edges:
            connected_nodes.add(e.get("source"))
            connected_nodes.add(e.get("target"))
        
        for node in nodes:
            if node["id"] not in connected_nodes:
                errors.append(ValidationError(
                    code="ORPHANED_NODE",
                    message=f"Node '{node['data'].get('label', node['id'])}' has no connections",
                    node_id=node["id"]
                ))
        
        return errors
    
    def _validate_port_types(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Ensure all connections have compatible types"""
        errors = []
        edges = graph.get("edges", [])
        nodes = {n["id"]: n for n in graph.get("nodes", [])}
        
        for edge in edges:
            edge_id = edge.get("id", "unknown")
            source_id = edge.get("source")
            target_id = edge.get("target")
            
            source_node = nodes.get(source_id)
            target_node = nodes.get(target_id)
            
            if not source_node:
                errors.append(ValidationError(
                    code="MISSING_SOURCE_NODE",
                    message=f"Edge references non-existent source node: {source_id}",
                    edge_id=edge_id
                ))
                continue
            
            if not target_node:
                errors.append(ValidationError(
                    code="MISSING_TARGET_NODE",
                    message=f"Edge references non-existent target node: {target_id}",
                    edge_id=edge_id
                ))
                continue
            
            # Get port types
            source_handle = edge.get("sourceHandle", "out")
            target_handle = edge.get("targetHandle", "in")
            
            source_ports = (source_node["data"].get("ports") or {})
            target_ports = (target_node["data"].get("ports") or {})

            source_type = source_ports.get("out")
            target_type = target_ports.get("in")

            
            if not source_type:
                errors.append(ValidationError(
                    code="MISSING_OUTPUT_PORT",
                    message=f"Source node '{source_node['data'].get('label')}' missing output port '{source_handle}'",
                    edge_id=edge_id,
                    node_id=source_id
                ))
                continue
            
            if not target_type:
                errors.append(ValidationError(
                    code="MISSING_INPUT_PORT",
                    message=f"Target node '{target_node['data'].get('label')}' missing input port '{target_handle}'",
                    edge_id=edge_id,
                    node_id=target_id
                ))
                continue
            
            # Check type compatibility
            if source_type != target_type:
                errors.append(ValidationError(
                    code="TYPE_MISMATCH",
                    message=f"Incompatible types: {source_type} -> {target_type} (from '{source_node['data'].get('label')}' to '{target_node['data'].get('label')}')",
                    edge_id=edge_id
                ))
        
        return errors
    
    def _validate_node_params(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Validate each node's parameters against expected schema"""
        errors = []
        nodes = graph.get("nodes", [])
        
        for node in nodes:
            node_id = node["id"]
            node_kind = node["data"].get("kind")
            params = node["data"].get("params", {})
            
            # Basic validation - you can expand this based on your schemas
            if node_kind == "source":
                # Source nodes might need file paths, URLs, etc.
                if not params:
                    errors.append(ValidationError(
                        code="MISSING_PARAMS",
                        message=f"Source node '{node['data'].get('label')}' requires parameters",
                        node_id=node_id
                    ))
            
            elif node_kind == "llm":
                # LLM nodes might need prompts, model names, etc.
                if not params.get("prompt") and not params.get("system"):
                    errors.append(ValidationError(
                        code="MISSING_PROMPT",
                        message=f"LLM node '{node['data'].get('label')}' requires a prompt or system message",
                        node_id=node_id
                    ))
        
        return errors
    
    def _check_resource_availability(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Check if required resources are available (warnings only)"""
        warnings = []
        nodes = graph.get("nodes", [])
        
        for node in nodes:
            node_id = node["id"]
            params = node["data"].get("params", {})
            
            # Check for file paths
            file_path = params.get("file_path") or params.get("path")
            if file_path:
                # In production, you'd actually check if file exists
                # For now, just warn if it looks suspicious
                if not isinstance(file_path, str) or len(file_path) == 0:
                    warnings.append(ValidationError(
                        code="INVALID_FILE_PATH",
                        message=f"Node '{node['data'].get('label')}' has invalid file path",
                        node_id=node_id
                    ))
        
        return warnings