# Marabunta - Licensed under the MIT License.
from marabunta_sdk.workflow import workflow, fortran_task, ForEach, TaskReference

# Write BROKEN Fortran code to disk (Missing !$MARABUNTA_BIND entirely)
with open("broken_climate.f90", "w") as f:
    f.write("""! Global Weather Simulation Kernel
subroutine calculate_entropy(pressure, velocity, entropy)
    implicit none
    real, intent(in) :: pressure, velocity
    real, intent(out) :: entropy
    
    entropy = (pressure * 2.0) + (velocity / 3.0)
end subroutine calculate_entropy
""")

@fortran_task(source_file="broken_climate.f90", subroutine="calculate_entropy")
def run_climate_sim(pressure: float, velocity: float):
    pass

@workflow(name="broken_weather_model")
def broken_dag(weather_chunks: list):
    with ForEach(items=weather_chunks, item_var="chunk"):
        run_climate_sim(pressure="${chunk.pressure}", velocity="${chunk.velocity}")

if __name__ == "__main__":
    broken_dag([])
