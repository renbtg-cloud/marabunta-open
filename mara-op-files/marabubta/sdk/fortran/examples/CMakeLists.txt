#==============================================================================
# Marabunta SDK Fortran Examples - CMake Configuration
#
# Builds example programs demonstrating the Fortran bindings.
#
# (c) 2024 Marabunta Compute Project
#==============================================================================

# Monte Carlo Pi estimation example
add_executable(monte_carlo monte_carlo.f90)
target_link_libraries(monte_carlo PRIVATE marabunta_sdk_fortran)

# Set output directory
set_target_properties(monte_carlo PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# Add a custom target to run the example
add_custom_target(run_monte_carlo
    COMMAND monte_carlo
    DEPENDS monte_carlo
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    COMMENT "Running Monte Carlo Pi estimation example"
)
