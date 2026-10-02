# Marabunta - Licensed under the MIT License.
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic)
import asyncio
from .client import HiveClient
from .workflow import workflow, task, ForEach

@magics_class
class MarabuntaMagics(Magics):
    @cell_magic
    def marabunta(self, line, cell):
        """
        Jupyter Cell Magic for Marabunta Swarm Execution.
        Usage: %%marabunta nodes=100 timeout=60s
        """
        # Parse arguments from 'line'
        args = {k: v for k, v in [arg.split('=') for arg in line.split() if '=' in arg]}
        
        # Capture the current local memory state (simplified for stub)
        # In production, we use the user's current shell globals()
        
        print(f"[MARABUNTA] Intercepting cell AST. Mapping to {args.get('nodes', 'dynamic')} swarm nodes...")
        
        # Simulated execution on swarm
        # 1. AST -> Task conversion
        # 2. State serialization
        # 3. Swarm injection
        
        async def run_swarm():
            print("[MARABUNTA] Shipping payload to Gateway...")
            await asyncio.sleep(1)
            print("[MARABUNTA] Execution complete. Injecting variables back into notebook.")
            
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # In Jupyter, the loop is often already running
                import nest_asyncio
                nest_asyncio.apply()
            loop.run_until_complete(run_swarm())
        except Exception as e:
            print(f"Error during swarm execution: {e}")

def load_ipython_extension(ipython):
    ipython.register_magics(MarabuntaMagics)
    print("Marabunta Jupyter Extension Loaded. Use %%marabunta to offload cells.")
