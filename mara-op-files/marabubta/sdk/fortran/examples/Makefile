# Marabunta - Licensed under the MIT License.
#==============================================================================
# Marabunta SDK Fortran Examples - Makefile
#
# Build examples demonstrating the Fortran bindings.
#
# Usage:
#   make              - Build all examples
#   make monte_carlo  - Build Monte Carlo example
#   make clean        - Remove build artifacts
#   make run          - Build and run Monte Carlo example
#
# Prerequisites:
#   - Fortran compiler (gfortran, ifort, or flang)
#   - Marabunta SDK library (libmarabunta_sdk)
#
# (c) 2024 Marabunta Compute Project
#==============================================================================

# Compiler selection (use FC environment variable if set)
FC ?= gfortran

# Compiler flags
FCFLAGS = -O2 -g -Wall -fcheck=all

# Add Fortran 2003/2008 standard compliance for different compilers
ifeq ($(FC),gfortran)
    FCFLAGS += -std=f2008 -fall-intrinsics
else ifeq ($(FC),ifort)
    FCFLAGS += -stand:f08
else ifeq ($(FC),ifx)
    FCFLAGS += -stand:f08
else ifeq ($(FC),flang)
    FCFLAGS += -std=f2008
endif

# Paths
SDK_SRC_DIR = ../src
SDK_LIB_DIR = ../lib
SDK_MOD_DIR = ../build
MARABUNTA_LIB_DIR ?= /usr/local/lib
MARABUNTA_INC_DIR ?= /usr/local/include

# Include paths for module files
INCLUDES = -I$(SDK_MOD_DIR) -I$(SDK_SRC_DIR)

# Library paths and libraries
LDFLAGS = -L$(SDK_LIB_DIR) -L$(MARABUNTA_LIB_DIR)
LIBS = -lmarabunta_sdk

# SDK module file
SDK_MODULE = $(SDK_MOD_DIR)/marabunta_sdk.mod
SDK_OBJECT = $(SDK_MOD_DIR)/marabunta_sdk.o

# Example programs
EXAMPLES = monte_carlo

# Default target
.PHONY: all
all: $(EXAMPLES)

# Build SDK module first
$(SDK_MODULE) $(SDK_OBJECT): $(SDK_SRC_DIR)/marabunta_sdk.f90
	@mkdir -p $(SDK_MOD_DIR)
	$(FC) $(FCFLAGS) -c $< -o $(SDK_OBJECT) -J$(SDK_MOD_DIR)

# Monte Carlo example
monte_carlo: monte_carlo.f90 $(SDK_OBJECT)
	$(FC) $(FCFLAGS) $(INCLUDES) -o $@ $< $(SDK_OBJECT) $(LDFLAGS) $(LIBS)

# Run the example
.PHONY: run
run: monte_carlo
	./monte_carlo

# Clean build artifacts
.PHONY: clean
clean:
	rm -f $(EXAMPLES) *.o *.mod
	rm -rf $(SDK_MOD_DIR)

# Install examples
.PHONY: install
install: $(EXAMPLES)
	install -d $(DESTDIR)$(prefix)/share/marabunta-sdk/examples/fortran
	install -m 755 $(EXAMPLES) $(DESTDIR)$(prefix)/share/marabunta-sdk/examples/fortran/
	install -m 644 *.f90 Makefile $(DESTDIR)$(prefix)/share/marabunta-sdk/examples/fortran/

# Debug build
.PHONY: debug
debug: FCFLAGS = -O0 -g -Wall -fcheck=all -fbacktrace
debug: clean all

# Help
.PHONY: help
help:
	@echo "Marabunta SDK Fortran Examples"
	@echo ""
	@echo "Targets:"
	@echo "  all          - Build all examples (default)"
	@echo "  monte_carlo  - Build Monte Carlo Pi estimation example"
	@echo "  run          - Build and run Monte Carlo example"
	@echo "  debug        - Build with debug flags"
	@echo "  clean        - Remove build artifacts"
	@echo "  install      - Install examples"
	@echo "  help         - Show this help"
	@echo ""
	@echo "Variables:"
	@echo "  FC           - Fortran compiler (default: gfortran)"
	@echo "  FCFLAGS      - Compiler flags"
	@echo "  MARABUNTA_LIB_DIR - Path to libmarabunta_sdk (default: /usr/local/lib)"
	@echo "  MARABUNTA_INC_DIR - Path to marabunta_sdk.h (default: /usr/local/include)"
