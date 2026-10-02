#==============================================================================
# Marabunta Task SDK - Fortran Bindings
#
# CMake build configuration for the Fortran module and examples.
#
# Usage:
#   mkdir build && cd build
#   cmake ..
#   make
#   make install
#
# Options:
#   -DBUILD_EXAMPLES=ON     Build example programs (default: ON)
#   -DBUILD_TESTING=ON      Build tests (default: OFF)
#   -DMARABUNTA_SDK_PATH=<path>  Path to marabunta_sdk library
#
# (c) 2024 Marabunta Compute Project
#==============================================================================

cmake_minimum_required(VERSION 3.14)

project(MarabuntaSDKFortran
    VERSION 1.0.0
    DESCRIPTION "Fortran bindings for the Marabunta Task SDK"
    LANGUAGES Fortran C)

# CMake module path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

#------------------------------------------------------------------------------
# Options
#------------------------------------------------------------------------------

option(BUILD_EXAMPLES "Build example programs" ON)
option(BUILD_TESTING "Build test programs" OFF)
option(INSTALL_SDK "Install the Fortran module and headers" ON)

#------------------------------------------------------------------------------
# Compiler settings
#------------------------------------------------------------------------------

# Fortran standard
set(CMAKE_Fortran_STANDARD 2008)
set(CMAKE_Fortran_STANDARD_REQUIRED ON)

# Compiler-specific flags
if(CMAKE_Fortran_COMPILER_ID MATCHES "GNU")
    set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -Wall -Wextra")
    set(CMAKE_Fortran_FLAGS_DEBUG "-O0 -g -fcheck=all -fbacktrace")
    set(CMAKE_Fortran_FLAGS_RELEASE "-O3 -march=native")
elseif(CMAKE_Fortran_COMPILER_ID MATCHES "Intel")
    set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -warn all")
    set(CMAKE_Fortran_FLAGS_DEBUG "-O0 -g -check all -traceback")
    set(CMAKE_Fortran_FLAGS_RELEASE "-O3 -xHost")
elseif(CMAKE_Fortran_COMPILER_ID MATCHES "Flang")
    set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -Wall")
    set(CMAKE_Fortran_FLAGS_DEBUG "-O0 -g")
    set(CMAKE_Fortran_FLAGS_RELEASE "-O3")
endif()

#------------------------------------------------------------------------------
# Find the Marabunta SDK C library
#------------------------------------------------------------------------------

# Try to find marabunta_sdk library
find_library(MARABUNTA_SDK_LIBRARY
    NAMES marabunta_sdk libmarabunta_sdk
    PATHS
        ${MARABUNTA_SDK_PATH}
        ${MARABUNTA_SDK_PATH}/lib
        /usr/local/lib
        /usr/lib
        $ENV{HOME}/.local/lib
    DOC "Path to the marabunta_sdk library"
)

# Try to find marabunta_sdk.h header
find_path(MARABUNTA_SDK_INCLUDE_DIR
    NAMES marabunta_sdk.h
    PATHS
        ${MARABUNTA_SDK_PATH}
        ${MARABUNTA_SDK_PATH}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        /usr/local/include
        /usr/include
        $ENV{HOME}/.local/include
    DOC "Path to marabunta_sdk.h header"
)

if(MARABUNTA_SDK_LIBRARY)
    message(STATUS "Found marabunta_sdk library: ${MARABUNTA_SDK_LIBRARY}")
else()
    message(WARNING "marabunta_sdk library not found - building Fortran module only")
    # Create a stub library target for development
    set(MARABUNTA_SDK_LIBRARY "")
endif()

if(MARABUNTA_SDK_INCLUDE_DIR)
    message(STATUS "Found marabunta_sdk headers: ${MARABUNTA_SDK_INCLUDE_DIR}")
endif()

#------------------------------------------------------------------------------
# Fortran Module Library
#------------------------------------------------------------------------------

# Create the Fortran module library
add_library(marabunta_sdk_fortran
    src/marabunta_sdk.f90
)

# Set module output directory
set(CMAKE_Fortran_MODULE_DIRECTORY ${CMAKE_BINARY_DIR}/modules)
target_include_directories(marabunta_sdk_fortran PUBLIC
    $<BUILD_INTERFACE:${CMAKE_Fortran_MODULE_DIRECTORY}>
    $<INSTALL_INTERFACE:include/marabunta_sdk>
)

# Link against the C library if found
if(MARABUNTA_SDK_LIBRARY)
    target_link_libraries(marabunta_sdk_fortran PUBLIC ${MARABUNTA_SDK_LIBRARY})
endif()

# Set library properties
set_target_properties(marabunta_sdk_fortran PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    OUTPUT_NAME marabunta_sdk_fortran
    Fortran_MODULE_DIRECTORY ${CMAKE_Fortran_MODULE_DIRECTORY}
)

# Alias for use in parent projects
add_library(MarabuntaSDK::Fortran ALIAS marabunta_sdk_fortran)

#------------------------------------------------------------------------------
# Examples
#------------------------------------------------------------------------------

if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

#------------------------------------------------------------------------------
# Tests
#------------------------------------------------------------------------------

if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()

#------------------------------------------------------------------------------
# Installation
#------------------------------------------------------------------------------

if(INSTALL_SDK)
    include(GNUInstallDirs)

    # Install the library
    install(TARGETS marabunta_sdk_fortran
        EXPORT MarabuntaSDKFortranTargets
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )

    # Install module files
    install(DIRECTORY ${CMAKE_Fortran_MODULE_DIRECTORY}/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/marabunta_sdk
        FILES_MATCHING PATTERN "*.mod"
    )

    # Install C header
    install(FILES include/marabunta_sdk.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/marabunta_sdk
    )

    # Install CMake config files
    install(EXPORT MarabuntaSDKFortranTargets
        FILE MarabuntaSDKFortranTargets.cmake
        NAMESPACE MarabuntaSDK::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/MarabuntaSDKFortran
    )

    # Create and install config file
    include(CMakePackageConfigHelpers)

    configure_package_config_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/MarabuntaSDKFortranConfig.cmake.in
        ${CMAKE_BINARY_DIR}/MarabuntaSDKFortranConfig.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/MarabuntaSDKFortran
    )

    write_basic_package_version_file(
        ${CMAKE_BINARY_DIR}/MarabuntaSDKFortranConfigVersion.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )

    install(FILES
        ${CMAKE_BINARY_DIR}/MarabuntaSDKFortranConfig.cmake
        ${CMAKE_BINARY_DIR}/MarabuntaSDKFortranConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/MarabuntaSDKFortran
    )

    # Install examples source
    if(BUILD_EXAMPLES)
        install(DIRECTORY examples/
            DESTINATION ${CMAKE_INSTALL_DATADIR}/marabunta_sdk/examples/fortran
            FILES_MATCHING
            PATTERN "*.f90"
            PATTERN "Makefile"
            PATTERN "CMakeLists.txt"
        )
    endif()
endif()

#------------------------------------------------------------------------------
# Package configuration summary
#------------------------------------------------------------------------------

message(STATUS "")
message(STATUS "Marabunta SDK Fortran Bindings Configuration:")
message(STATUS "  Version:          ${PROJECT_VERSION}")
message(STATUS "  Fortran compiler: ${CMAKE_Fortran_COMPILER_ID} ${CMAKE_Fortran_COMPILER_VERSION}")
message(STATUS "  Build type:       ${CMAKE_BUILD_TYPE}")
message(STATUS "  Build examples:   ${BUILD_EXAMPLES}")
message(STATUS "  Build tests:      ${BUILD_TESTING}")
message(STATUS "  Install:          ${INSTALL_SDK}")
message(STATUS "  Install prefix:   ${CMAKE_INSTALL_PREFIX}")
message(STATUS "")
