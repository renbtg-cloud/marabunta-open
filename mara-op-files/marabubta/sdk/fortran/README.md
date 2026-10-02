<!-- Marabunta - Licensed under the MIT License.
# Marabunta Task SDK - Fortran Bindings

Modern Fortran 2003+ bindings for the Marabunta distributed compute cluster SDK.

This module provides a clean, idiomatic Fortran interface to the Marabunta Task C ABI,
enabling scientific simulations written in Fortran to participate in distributed
computation with progress reporting, checkpointing, and pause/resume support.

## Features

- **Modern Fortran OOP** - Type-bound procedures for clean object-oriented code
- **Legacy F77 Support** - Standalone subroutines for older codebases
- **ISO_C_BINDING** - Standard-compliant C interoperability
- **Automatic String Handling** - Seamless Fortran-to-C string conversion
- **Optional Error Codes** - Fortran-style `ierr` optional arguments
- **Type Safety** - Proper `c_int`, `c_ptr`, etc. kind parameters

## Requirements

- Fortran 2003+ compiler (gfortran 4.9+, ifort 15+, or flang)
- CMake 3.14+ (for CMake build)
- Marabunta SDK C library (`libmarabunta_sdk`)

## Quick Start

### Object-Oriented Style (Recommended)

```fortran
program my_computation
    use marabunta_sdk
    use, intrinsic :: iso_c_binding
    implicit none

    type(marabunta_context) :: task
    integer :: i, ierr, yield_result

    ! Initialize task context
    call task%init(ierr)
    if (ierr /= 0) stop "Failed to initialize"

    ! Set stage name and expected runtime
    call task%set_stage("Processing data")
    call task%hint_time(120)  ! ~2 minutes
    call task%hint_memory(int(1024*1024*100, 8))  ! 100 MB

    ! Check for checkpoint to resume
    if (task%checkpoint_exists()) then
        ! Load saved state...
    end if

    ! Main computation loop
    do i = 1, 1000000
        ! ... do work ...

        ! Report progress every 10000 iterations
        if (mod(i, 10000) == 0) then
            call task%set_progress(i * 100 / 1000000)

            ! Check for pause/abort
            call task%yield(yield_result)
            if (yield_result == MARABUNTA_YIELD_ABORT) then
                call task%cleanup()
                stop "Aborted"
            else if (yield_result == MARABUNTA_YIELD_PAUSE) then
                ! Save state for resume
                call task%save_checkpoint(c_loc(state), &
                    int(c_sizeof(state), c_size_t))
                call task%cleanup()
                stop "Paused"
            end if
        end if
    end do

    ! Report result and cleanup
    call task%set_result_real(result_value)
    call task%cleanup()

end program
```

### Legacy F77 Style

```fortran
      PROGRAM LEGACY
      USE MARABUNTA_SDK
      IMPLICIT NONE

      INTEGER IERR, YRES

C     Initialize
      CALL MARABUNTA_INIT(IERR)
      CALL MARABUNTA_STAGE('Computing')

C     Work loop
      DO 100 I = 1, 1000
          ! ... compute ...
          CALL MARABUNTA_PROGRESS(I * 100 / 1000)
          CALL MARABUNTA_YIELD(YRES)
          IF (YRES .NE. MARABUNTA_YIELD_CONTINUE) GOTO 200
  100 CONTINUE

C     Done
  200 CALL MARABUNTA_SET_RESULT_REAL(3.14159D0)
      CALL MARABUNTA_FINALIZE()

      END
```

### Distributed Task Example

```fortran
program distributed_task
    use marabunta_sdk
    implicit none

    type(marabunta_context) :: task
    integer :: ierr, rank, nworkers

    ! Initialize with specific task parameters
    call task%init_ex( &
        task_id=12345_8, &
        worker_rank=0, &
        worker_count=4, &
        checkpoint_dir="/tmp/checkpoints")

    ! Get worker info
    rank = task%get_worker_rank()
    nworkers = task%get_worker_count()

    write(*,'(A,I0,A,I0)') "Worker ", rank, " of ", nworkers

    ! Hint that this task can be split
    call task%hint_split(min_chunks=2, max_chunks=16)

    ! ... do distributed work ...

    call task%cleanup()
end program
```

## Building

### With CMake (Recommended)

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make
make install  # optional
```

CMake options:
- `-DBUILD_EXAMPLES=ON` - Build example programs (default: ON)
- `-DBUILD_TESTING=ON` - Build tests (default: OFF)
- `-DMARABUNTA_SDK_PATH=/path/to/sdk` - Path to marabunta_sdk library

### With Make

```bash
cd examples
make FC=gfortran
./monte_carlo
```

## API Reference

### Types

#### `marabunta_context`

Opaque handle to the task context. Must be initialized before use.

```fortran
type :: marabunta_context
    ! Internal handle - do not access directly
contains
    ! Lifecycle
    procedure :: init
    procedure :: init_ex
    procedure :: cleanup
    procedure :: is_valid

    ! Progress reporting
    procedure :: set_progress
    procedure :: increment_progress
    procedure :: get_progress
    procedure :: set_stage
    procedure :: set_message
    procedure :: hint_time
    procedure :: hint_memory
    procedure :: hint_split

    ! Yield control
    procedure :: yield
    procedure :: should_pause
    procedure :: should_abort

    ! Checkpointing
    procedure :: save_checkpoint
    procedure :: load_checkpoint
    procedure :: checkpoint_exists
    procedure :: save_checkpoint_int
    procedure :: load_checkpoint_int
    procedure :: save_checkpoint_real
    procedure :: load_checkpoint_real
    procedure :: save_checkpoint_array
    procedure :: load_checkpoint_array

    ! Results
    procedure :: set_result
    procedure :: set_result_string
    procedure :: set_result_json
    procedure :: set_result_int
    procedure :: set_result_real
    procedure :: append_result
    procedure :: get_result_size

    ! Logging
    procedure :: log
    procedure :: log_debug
    procedure :: log_info
    procedure :: log_warn
    procedure :: log_error

    ! Task info
    procedure :: get_task_id
    procedure :: get_worker_rank
    procedure :: get_worker_count
end type
```

### Constants

#### Error Codes

| Constant | Value | Description |
|----------|-------|-------------|
| `MARABUNTA_OK` | 0 | Success |
| `MARABUNTA_ERR_NULL_CONTEXT` | -1 | Null context pointer |
| `MARABUNTA_ERR_NULL_POINTER` | -2 | Null pointer argument |
| `MARABUNTA_ERR_INVALID_UTF8` | -3 | Invalid UTF-8 string |
| `MARABUNTA_ERR_IO` | -4 | I/O error |
| `MARABUNTA_ERR_CHECKPOINT_NOT_FOUND` | -5 | Checkpoint not found |
| `MARABUNTA_ERR_BUFFER_TOO_SMALL` | -6 | Buffer too small |
| `MARABUNTA_ERR_ALREADY_INITIALIZED` | -7 | Already initialized |
| `MARABUNTA_ERR_NOT_INITIALIZED` | -8 | Not initialized |
| `MARABUNTA_ERR_ABORTED` | -9 | Task aborted |
| `MARABUNTA_ERR_INVALID_ARGUMENT` | -10 | Invalid argument |
| `MARABUNTA_ERR_ALLOCATION_FAILED` | -11 | Memory allocation failed |
| `MARABUNTA_ERR_TIMEOUT` | -12 | Operation timed out |
| `MARABUNTA_ERR_PANIC` | -99 | Internal error |

#### Yield Results

| Constant | Value | Description |
|----------|-------|-------------|
| `MARABUNTA_YIELD_CONTINUE` | 0 | Continue execution |
| `MARABUNTA_YIELD_PAUSE` | 1 | Save state and pause |
| `MARABUNTA_YIELD_ABORT` | 2 | Abort the task |

#### Log Levels

| Constant | Value | Description |
|----------|-------|-------------|
| `MARABUNTA_LOG_DEBUG` | 0 | Debug messages |
| `MARABUNTA_LOG_INFO` | 1 | Informational |
| `MARABUNTA_LOG_WARN` | 2 | Warnings |
| `MARABUNTA_LOG_ERROR` | 3 | Errors |

### Procedures

#### Context Management

```fortran
subroutine init(this, ierr)
    ! Initialize the task context with default settings
    class(marabunta_context), intent(inout) :: this
    integer, intent(out), optional :: ierr

subroutine init_ex(this, task_id, worker_rank, worker_count, checkpoint_dir, ierr)
    ! Initialize with specific parameters
    class(marabunta_context), intent(inout) :: this
    integer(8), intent(in) :: task_id
    integer, intent(in) :: worker_rank
    integer, intent(in) :: worker_count
    character(len=*), intent(in), optional :: checkpoint_dir
    integer, intent(out), optional :: ierr

subroutine cleanup(this)
    ! Release the task context
    class(marabunta_context), intent(inout) :: this

function is_valid(this) result(valid)
    ! Check if context is valid
    class(marabunta_context), intent(in) :: this
    logical :: valid
```

#### Progress Reporting

```fortran
subroutine set_progress(this, percent, ierr)
    ! Set progress percentage (0-100)
    class(marabunta_context), intent(inout) :: this
    integer, intent(in) :: percent
    integer, intent(out), optional :: ierr

subroutine increment_progress(this, delta, ierr)
    ! Increment progress by delta
    class(marabunta_context), intent(inout) :: this
    integer, intent(in) :: delta
    integer, intent(out), optional :: ierr

function get_progress(this) result(percent)
    ! Get current progress
    class(marabunta_context), intent(inout) :: this
    integer :: percent

subroutine set_stage(this, stage_name, ierr)
    ! Set current stage name
    class(marabunta_context), intent(inout) :: this
    character(len=*), intent(in) :: stage_name
    integer, intent(out), optional :: ierr

subroutine set_message(this, message, ierr)
    ! Set progress message
    class(marabunta_context), intent(inout) :: this
    character(len=*), intent(in) :: message
    integer, intent(out), optional :: ierr

subroutine hint_time(this, seconds, ierr)
    ! Hint expected total runtime
    class(marabunta_context), intent(inout) :: this
    integer, intent(in) :: seconds
    integer, intent(out), optional :: ierr

subroutine hint_memory(this, bytes, ierr)
    ! Hint memory requirements
    class(marabunta_context), intent(inout) :: this
    integer(8), intent(in) :: bytes
    integer, intent(out), optional :: ierr

subroutine hint_split(this, min_chunks, max_chunks, ierr)
    ! Hint task can be parallelized
    class(marabunta_context), intent(inout) :: this
    integer, intent(in) :: min_chunks
    integer, intent(in) :: max_chunks
    integer, intent(out), optional :: ierr
```

#### Yield Control

```fortran
subroutine yield(this, result, ierr)
    ! Check for pause/abort requests
    class(marabunta_context), intent(inout) :: this
    integer, intent(out) :: result  ! MARABUNTA_YIELD_*
    integer, intent(out), optional :: ierr

function should_pause(this) result(should)
    ! Check if task should pause
    class(marabunta_context), intent(inout) :: this
    logical :: should

function should_abort(this) result(should)
    ! Check if task should abort
    class(marabunta_context), intent(inout) :: this
    logical :: should
```

#### Checkpointing

```fortran
subroutine save_checkpoint(this, data, size, ierr)
    ! Save binary checkpoint data
    class(marabunta_context), intent(inout) :: this
    type(c_ptr), intent(in) :: data
    integer(c_size_t), intent(in) :: size
    integer, intent(out), optional :: ierr

subroutine load_checkpoint(this, buffer, buffer_size, bytes_read, ierr)
    ! Load checkpoint data
    class(marabunta_context), intent(inout) :: this
    type(c_ptr), intent(in) :: buffer
    integer(c_size_t), intent(in) :: buffer_size
    integer(8), intent(out) :: bytes_read
    integer, intent(out), optional :: ierr

function checkpoint_exists(this) result(exists)
    ! Check if checkpoint exists
    class(marabunta_context), intent(inout) :: this
    logical :: exists

! Convenience methods for common types
subroutine save_checkpoint_int(this, value, ierr)
subroutine load_checkpoint_int(this, value, ierr)
subroutine save_checkpoint_real(this, value, ierr)
subroutine load_checkpoint_real(this, value, ierr)
subroutine save_checkpoint_array(this, array, ierr)
subroutine load_checkpoint_array(this, array, ierr)
```

#### Results

```fortran
subroutine set_result(this, data, size, ierr)
    ! Set binary result
subroutine set_result_int(this, value, ierr)
    ! Set integer result (as JSON number)
subroutine set_result_real(this, value, ierr)
    ! Set real(8) result (as JSON number)
subroutine set_result_string(this, value, ierr)
    ! Set string result (as JSON string)
subroutine set_result_json(this, json_string, ierr)
    ! Set JSON result directly
subroutine append_result(this, data, size, ierr)
    ! Append data to result
function get_result_size(this) result(size)
    ! Get current result size
```

#### Logging

```fortran
subroutine log(this, level, message, ierr)
subroutine log_debug(this, message, ierr)
subroutine log_info(this, message, ierr)
subroutine log_warn(this, message, ierr)
subroutine log_error(this, message, ierr)
```

#### Task Information

```fortran
function get_task_id(this) result(task_id)
    class(marabunta_context), intent(inout) :: this
    integer(8) :: task_id

function get_worker_rank(this) result(rank)
    class(marabunta_context), intent(inout) :: this
    integer :: rank

function get_worker_count(this) result(count)
    class(marabunta_context), intent(inout) :: this
    integer :: count
```

### Utility Functions

```fortran
function marabunta_get_error_message(code) result(message)
    ! Get human-readable error message
    integer, intent(in) :: code
    character(len=:), allocatable :: message

function marabunta_get_version() result(version)
    ! Get SDK version string
    character(len=:), allocatable :: version
```

### Legacy F77-Style Subroutines

For compatibility with older codebases, standalone subroutines are provided
that operate on a global context:

```fortran
! Lifecycle
subroutine marabunta_init(ierr)
subroutine marabunta_init_ex(task_id, worker_rank, worker_count, checkpoint_dir, ierr)
subroutine marabunta_finalize()

! Progress
subroutine marabunta_progress(percent, ierr)
subroutine marabunta_progress_increment(delta, ierr)
function marabunta_progress_get() result(percent)
subroutine marabunta_stage(stage_name, ierr)
subroutine marabunta_message(message, ierr)

! Yield
subroutine marabunta_yield(result, ierr)
function marabunta_should_pause() result(should)
function marabunta_should_abort() result(should)

! Checkpointing
subroutine marabunta_save_checkpoint(data, size, ierr)
subroutine marabunta_load_checkpoint(buffer, buffer_size, bytes_read, ierr)
function marabunta_checkpoint_exists() result(exists)

! Results
subroutine marabunta_set_result(data, size, ierr)
subroutine marabunta_set_result_int(value, ierr)
subroutine marabunta_set_result_real(value, ierr)
subroutine marabunta_set_result_string(value, ierr)
subroutine marabunta_set_result_json(json_string, ierr)

! Logging
subroutine marabunta_log_message(level, message, ierr)

! Task info
function marabunta_get_task_id() result(task_id)
function marabunta_get_worker_rank() result(rank)
function marabunta_get_worker_count() result(count)

! Hints
subroutine marabunta_hint_memory(bytes, ierr)
subroutine marabunta_hint_time(seconds, ierr)
subroutine marabunta_hint_split(min_chunks, max_chunks, ierr)
```

## Examples

### Monte Carlo Pi Estimation

See `examples/monte_carlo.f90` for a complete example demonstrating:
- Task initialization
- Progress reporting
- Checkpoint save/resume with structured data
- Pause/abort handling
- Result reporting
- Worker rank/count queries

## Directory Structure

```
sdk/fortran/
├── src/
│   └── marabunta_sdk.f90      # Fortran module with ISO_C_BINDING
├── include/
│   └── marabunta_sdk.h        # C header for reference
├── examples/
│   ├── monte_carlo.f90   # Example Monte Carlo simulation
│   ├── Makefile          # Example build with Make
│   └── CMakeLists.txt    # Example build with CMake
├── cmake/
│   └── MarabuntaSDKFortranConfig.cmake.in
├── CMakeLists.txt        # Main CMake configuration
└── README.md             # This file
```

## License

Copyright (c) 2024 Marabunta Compute Project. All rights reserved.
