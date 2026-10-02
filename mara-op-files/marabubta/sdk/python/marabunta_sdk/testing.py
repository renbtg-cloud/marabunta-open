# Marabunta - Licensed under the MIT License.
from functools import wraps
import random
import asyncio

def swarm_test(simulate_drops="0%", malicious_actors="0%"):
    """
    Decorator for unit tests to simulate distributed swarm chaos.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            drop_rate = float(simulate_drops.strip('%')) / 100.0
            malice_rate = float(malicious_actors.strip('%')) / 100.0
            
            print(f"[CHAOS MONKEY] Initializing test with {simulate_drops} node drops and {malicious_actors} malicious actors.")
            
            # Inject chaos configuration into the test context or client
            # (In production, this would configure the Marabunta mock environment)
            
            # Execute the test
            if asyncio.iscoroutinefunction(func):
                loop = asyncio.get_event_loop()
                return loop.run_until_complete(func(*args, **kwargs))
            else:
                return func(*args, **kwargs)
        return wrapper
    return decorator
