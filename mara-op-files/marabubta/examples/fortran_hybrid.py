# Marabunta - Licensed under the MIT License.
from marabunta_sdk.workflow import workflow, fortran_task, ForEach, TaskReference

# Write the actual Fortran code to disk (what the scientist would do)
with open("climate.f90", "w") as f:
    f.write("""! Global Weather Simulation Kernel
!$MARABUNTA_BIND INPUT=(pressure, velocity) OUTPUT=(entropy)
subroutine calculate_entropy(pressure, velocity, entropy)
    implicit none
    real, intent(in) :: pressure, velocity
    real, intent(out) :: entropy
    
    ! Extremely complex atmospheric math
    entropy = (pressure * 2.0) + (velocity / 3.0)
end subroutine calculate_entropy
""")

# The Python Orchestrator DAG
@fortran_task(source_file="climate.f90", subroutine="calculate_entropy", memory_min=2048)
def run_climate_sim(pressure: float, velocity: float):
    # Pure Python type-enforcement and validation boundary.
    pass

@workflow(name="global_weather_model")
def weather_dag(weather_chunks: list):
    # Spray the compiled Fortran WASM across 10,000 nodes natively
    with ForEach(items=weather_chunks, item_var="chunk"):
        run_climate_sim(
            pressure="${chunk.pressure}", 
            velocity="${chunk.velocity}"
        )

if __name__ == "__main__":
    import json
    # Generating the JSON payload expected by the Marabunta Coordinator
    dag_json = weather_dag([{"pressure": 10.0, "velocity": 5.0}])
    print("\n--- DAG JSON ---")
    print(json.dumps(dag_json, indent=2))
