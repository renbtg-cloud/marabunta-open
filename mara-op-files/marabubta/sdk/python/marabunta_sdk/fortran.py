# Marabunta - Licensed under the MIT License.
import os
import re
from typing import Dict, List, Any, Optional, Tuple

class FortranBindError(Exception):
    """Raised when a !$MARABUNTA_BIND directive is malformed or missing."""
    pass

class FortranSignature:
    """Represents the parsed ABI interface of a Fortran subroutine."""
    def __init__(self, name: str, inputs: List[str], outputs: List[str]):
        self.name = name
        self.inputs = inputs
        self.outputs = outputs

def _parse_marabunta_bind(filepath: str, target_subroutine: str) -> FortranSignature:
    """
    Parses a .f90 file looking for:
    !$MARABUNTA_BIND INPUT=(var1, var2) OUTPUT=(res)
    subroutine target_subroutine(...)
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"[FATAL] Fortran source file not found: {filepath}")

    with open(filepath, "r") as f:
        lines = f.readlines()

    bind_pattern = re.compile(r"!\$MARABUNTA_BIND\s+INPUT=\((.*?)\)\s+OUTPUT=\((.*?)\)", re.IGNORECASE)
    subroutine_pattern = re.compile(r"^\s*subroutine\s+(\w+)", re.IGNORECASE)

    current_bind: Optional[Tuple[List[str], List[str]]] = None

    for i, line in enumerate(lines):
        line = line.strip()
        
        # Look for the directive
        bind_match = bind_pattern.match(line)
        if bind_match:
            inputs = [x.strip() for x in bind_match.group(1).split(",") if x.strip()]
            outputs = [x.strip() for x in bind_match.group(2).split(",") if x.strip()]
            current_bind = (inputs, outputs)
            continue

        # Look for the subroutine
        sub_match = subroutine_pattern.match(line)
        if sub_match:
            sub_name = sub_match.group(1)
            if sub_name.lower() == target_subroutine.lower():
                if not current_bind:
                    raise FortranBindError(
                        f"[{filepath}:{i+1}] [FATAL] Missing !$MARABUNTA_BIND directive immediately preceding subroutine '{target_subroutine}'. "
                        f"You must define the Swarm ABI. Example: !$MARABUNTA_BIND INPUT=(x,y) OUTPUT=(z)"
                    )
                return FortranSignature(name=sub_name, inputs=current_bind[0], outputs=current_bind[1])
            
            # Reset bind if it was attached to the wrong subroutine
            current_bind = None

    raise FortranBindError(f"[FATAL] Subroutine '{target_subroutine}' not found in {filepath}.")

def generate_wasm_c_wrapper(signature: FortranSignature, python_kwargs: Dict[str, Any]) -> str:
    """
    Generates the C wrapper that bridges the Marabunta JSON payload (from stdin) 
    to the raw Fortran memory pointers.
    
    (In a full implementation, this maps types like double/int natively).
    """
    # Verify the Python inputs match the Fortran !$MARABUNTA_BIND INPUTs
    missing = [req for req in signature.inputs if req not in python_kwargs and not req.startswith("arg_")]
    if missing:
        raise ValueError(
            f"[FATAL] ABI Mismatch! Python @fortran_task is missing arguments required by the Fortran !$MARABUNTA_BIND directive: {missing}"
        )

    # Note: In production, this generates actual C code and invokes LLVM (clang/flang) 
    # to output a standalone .wasm module. We mock the generated wrapper logic here for the DAG.
    return f"""
    // AUTO-GENERATED SWARM C-WRAPPER FOR FORTRAN SUBROUTINE: {signature.name}
    // INPUTS: {', '.join(signature.inputs)}
    // OUTPUTS: {', '.join(signature.outputs)}
    // (LLVM/Flang WASM compilation happens here)
    """
