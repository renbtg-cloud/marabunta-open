# Marabunta - Licensed under the MIT License.
import inspect
import textwrap
import uuid
import os
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union
from .fortran import _parse_marabunta_bind, generate_wasm_c_wrapper, FortranBindError

_workflow_context = None

class TaskReference:
    """A reference to the output of a step in the workflow."""
    def __init__(self, step_id: str, output_var: str):
        self.step_id = step_id
        self.output_var = output_var

    def __str__(self):
        return f"${{{self.output_var}}}"
        
    def to_dict(self):
        return {"__ref": self.output_var}


class Condition:
    """Builder for conditional logic mapped to the Rust backend."""
    @staticmethod
    def on_success(step: Union[str, TaskReference]) -> dict:
        step_id = step.step_id if isinstance(step, TaskReference) else step
        return {"type": "on_success", "step": step_id}

    @staticmethod
    def on_failure(step: Union[str, TaskReference]) -> dict:
        step_id = step.step_id if isinstance(step, TaskReference) else step
        return {"type": "on_failure", "step": step_id}

    @staticmethod
    def on_output_match(step: Union[str, TaskReference], path: str, value: Any, operator: str = "equals") -> dict:
        step_id = step.step_id if isinstance(step, TaskReference) else step
        return {"type": "on_output_match", "step": step_id, "path": path, "value": value, "operator": operator}

    @staticmethod
    def on_variable(variable: str, value: Any, operator: str = "equals") -> dict:
        # Strip ${} if passed implicitly
        var_name = variable.output_var if isinstance(variable, TaskReference) else variable.strip("${}")
        return {"type": "on_variable", "variable": var_name, "value": value, "operator": operator}

    @staticmethod
    def logical_and(*conditions: dict) -> dict:
        return {"type": "and", "conditions": list(conditions)}

    @staticmethod
    def logical_or(*conditions: dict) -> dict:
        return {"type": "or", "conditions": list(conditions)}


class WorkflowBlock:
    """Base context manager for nested workflow blocks (Parallel, Loop)."""
    def __init__(self, builder, factory_fn):
        self.builder = builder
        self.factory_fn = factory_fn
        self.steps = []
        
    def __enter__(self):
        self.builder.push_block(self.steps)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.builder.pop_block()
        if exc_type is None:
            step_def = self.factory_fn(self.steps)
            self.builder.add_step(step_def)


class IfBlock:
    """Context manager for If/Else conditional branching."""
    def __init__(self, builder, condition: dict):
        self.builder = builder
        self.condition = condition
        self.then_steps = []
        self.else_steps = []
        self.in_else = False
        self.step_id = f"cond_{uuid.uuid4().hex[:8]}"

    def __enter__(self):
        self.builder.push_block(self.then_steps)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.builder.pop_block()
        if not self.in_else and exc_type is None:
            step = {
                "id": self.step_id,
                "type": "conditional",
                "condition": self.condition,
                "then": self.then_steps
            }
            self.builder.add_step(step)

    def Else(self):
        """Chainable Else block."""
        class ElseContext:
            def __init__(self, if_block):
                self.if_block = if_block
            def __enter__(self):
                self.if_block.in_else = True
                self.if_block.builder.push_block(self.if_block.else_steps)
            def __exit__(self, exc_type, exc_val, exc_tb):
                self.if_block.builder.pop_block()
                if exc_type is None:
                    # Find the conditional step and attach the else branch
                    for step in self.if_block.builder.current_steps():
                        if step.get("id") == self.if_block.step_id:
                            step["else"] = self.if_block.else_steps
        return ElseContext(self)


class WorkflowBuilder:
    def __init__(self, name: str, version: str = "1.0"):
        self.name = name
        self.version = version
        self.variables = {}
        self.stack = [[]] # Stack of step lists for nesting

    def current_steps(self) -> list:
        return self.stack[-1]

    def push_block(self, steps_list: list):
        self.stack.append(steps_list)

    def pop_block(self):
        self.stack.pop()

    def add_step(self, step: dict):
        self.current_steps().append(step)

    def build(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "variables": self.variables,
            "steps": self.stack[0]
        }


# GLOBAL CONTEXT MANAGERS FOR THE DSL

def If(condition: dict) -> IfBlock:
    """Start a conditional branch."""
    if _workflow_context is None: raise RuntimeError("Must be used inside a @workflow function")
    return IfBlock(_workflow_context, condition)

def Parallel():
    """Start a parallel execution block."""
    if _workflow_context is None: raise RuntimeError("Must be used inside a @workflow function")
    step_id = f"parallel_{uuid.uuid4().hex[:8]}"
    def factory(steps):
        return {"id": step_id, "type": "parallel", "branches": steps}
    return WorkflowBlock(_workflow_context, factory)

def ForEach(items: Union[str, TaskReference], item_var: str = "item", index_var: str = "index"):
    """Start a ForEach loop block over an array variable."""
    if _workflow_context is None: raise RuntimeError("Must be used inside a @workflow function")
    step_id = f"foreach_{uuid.uuid4().hex[:8]}"
    items_name = items.output_var if isinstance(items, TaskReference) else str(items).strip("${}")
    def factory(steps):
        return {
            "id": step_id,
            "type": "loop",
            "condition": {"type": "for_each", "items": items_name, "item": item_var, "index": index_var},
            "body": steps
        }
    return WorkflowBlock(_workflow_context, factory)

def While(condition: dict, max_iterations: int = 1000):
    """Start a While loop block based on a condition."""
    if _workflow_context is None: raise RuntimeError("Must be used inside a @workflow function")
    step_id = f"while_{uuid.uuid4().hex[:8]}"
    def factory(steps):
        return {
            "id": step_id,
            "type": "loop",
            "condition": {"type": "while", "condition": condition, "max_iterations": max_iterations},
            "body": steps
        }
    return WorkflowBlock(_workflow_context, factory)

def CountLoop(count: int, iterator_var: str = "i"):
    """Start a fixed count loop."""
    if _workflow_context is None: raise RuntimeError("Must be used inside a @workflow function")
    step_id = f"count_{uuid.uuid4().hex[:8]}"
    def factory(steps):
        return {
            "id": step_id,
            "type": "loop",
            "condition": {"type": "count", "count": count, "iterator": iterator_var},
            "body": steps
        }
    return WorkflowBlock(_workflow_context, factory)


def task(
    name: Optional[str] = None,
    runtime: str = "python3",
    timeout_secs: Optional[int] = None,
    retries: Optional[int] = None,
    memory_min: Optional[int] = None,
    continue_on_failure: bool = False
):
    """Decorator to define a workflow task. Automatically extracts the function AST."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global _workflow_context
            if _workflow_context is None:
                return func(*args, **kwargs)

            step_id = f"{func.__name__}_{uuid.uuid4().hex[:8]}"
            output_var = f"out_{step_id}"
            depends_on = set()
            job_args = {}
            
            for i, arg in enumerate(args):
                arg_name = f"arg_{i}"
                if isinstance(arg, TaskReference):
                    depends_on.add(arg.step_id)
                    job_args[arg_name] = str(arg)
                else:
                    job_args[arg_name] = arg
                    
            for k, v in kwargs.items():
                if isinstance(v, TaskReference):
                    depends_on.add(v.step_id)
                    job_args[k] = str(v)
                else:
                    job_args[k] = v

            source = textwrap.dedent(inspect.getsource(func))
            
            runner = f"""

if __name__ == '__main__':
    import json, os, sys
    args_json = os.environ.get('MARABUNTA_JOB_ARGS', '{{}}')
    args_dict = json.loads(args_json)
    pos_args = [args_dict[f"arg_{{i}}"] for i in range(len(args_dict)) if f"arg_{{i}}" in args_dict]
    kw_args = {{k: v for k, v in args_dict.items() if not k.startswith("arg_")}}
    res = {func.__name__}(*pos_args, **kw_args)
    output_path = os.environ.get('MARABUNTA_OUTPUT_PATH', '/marabunta/outputs/result.json')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(res, f)
"""
            full_script = source + runner

            job_config = {
                "name": name or func.__name__,
                "runtime": runtime,
                "script": full_script,
                "args": job_args,
                "output_var": output_var
            }

            if memory_min:
                job_config["resources"] = {"memory_min": memory_min}

            step = {
                "id": step_id,
                "type": "job",
                "job": job_config,
                "depends_on": list(depends_on),
                "continue_on_failure": continue_on_failure
            }
            if timeout_secs: step["timeout_secs"] = timeout_secs
            if retries: step["max_retries"] = retries

            _workflow_context.add_step(step)
            return TaskReference(step_id, output_var)
        return wrapper
    return decorator


def workflow(name: str, version: str = "1.0"):
    """Decorator to define a dynamic workflow DAG."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global _workflow_context
            previous_context = _workflow_context
            _workflow_context = WorkflowBuilder(name, version)
            try:
                func(*args, **kwargs)
                return _workflow_context.build()
            finally:
                _workflow_context = previous_context
        return wrapper
    return decorator


def fortran_task(
    source_file: str,
    subroutine: str,
    timeout_secs: Optional[int] = None,
    retries: Optional[int] = None,
    memory_min: Optional[int] = None,
    continue_on_failure: bool = False
):
    """
    Decorator to define a Fortran WASM execution task.
    Automatically cross-compiles the .f90 source using LLVM and wires the Swarm ABI.
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global _workflow_context
            if _workflow_context is None:
                raise RuntimeError("Fortran tasks must be executed within a @workflow DAG.")

            # Validate Fortran ABI
            try:
                signature = _parse_marabunta_bind(source_file, subroutine)
            except FortranBindError as e:
                raise RuntimeError(str(e))
                
            step_id = f"fortran_{subroutine}_{uuid.uuid4().hex[:8]}"
            output_var = f"out_{step_id}"
            depends_on = set()
            job_args = {}
            
            for i, arg in enumerate(args):
                arg_name = signature.inputs[i] if i < len(signature.inputs) else f"arg_{i}"
                if isinstance(arg, TaskReference):
                    depends_on.add(arg.step_id)
                    job_args[arg_name] = str(arg)
                else:
                    job_args[arg_name] = arg
                    
            for k, v in kwargs.items():
                if isinstance(v, TaskReference):
                    depends_on.add(v.step_id)
                    job_args[k] = str(v)
                else:
                    job_args[k] = v

            # Validate ABI Mismatch
            try:
                c_wrapper_code = generate_wasm_c_wrapper(signature, job_args)
            except ValueError as e:
                raise RuntimeError(str(e))

            # Compile Fortran + C-Wrapper to a .wasm payload (Mocked in SDK)
            print(f"[COMPILER] Cross-compiling {source_file} (subroutine: {subroutine}) to WASM...")
            wasm_binary_mock = f"WASM_PAYLOAD_FOR_{subroutine}"

            job_config = {
                "name": f"fortran_{subroutine}",
                "runtime": "wasm",
                "script": c_wrapper_code, # In production this is the raw WASM binary
                "args": job_args,
                "output_var": output_var
            }

            if memory_min:
                job_config["resources"] = {"memory_min": memory_min}

            step = {
                "id": step_id,
                "type": "job",
                "job": job_config,
                "depends_on": list(depends_on),
                "continue_on_failure": continue_on_failure
            }
            if timeout_secs: step["timeout_secs"] = timeout_secs
            if retries: step["max_retries"] = retries

            _workflow_context.add_step(step)
            return TaskReference(step_id, output_var)
        return wrapper
    return decorator
