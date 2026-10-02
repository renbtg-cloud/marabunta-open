! Marabunta - Licensed under the MIT License.
!==============================================================================
! Marabunta Task SDK - Fortran Module
!
! Modern Fortran 2003+ bindings for the Marabunta distributed compute cluster.
! Uses ISO_C_BINDING for C interoperability.
!
! Features:
!   - Object-oriented interface with type-bound procedures
!   - Legacy F77-style standalone subroutines
!   - Automatic string conversion (Fortran <-> C)
!   - Optional error handling arguments
!
! Example usage:
!   use marabunta_sdk
!   type(marabunta_context) :: task
!
!   call task%init()
!   call task%set_stage("Processing data")
!   call task%set_progress(50)
!   call task%cleanup()
!
! (c) 2024 Marabunta Compute Project
!==============================================================================

module marabunta_sdk
    use, intrinsic :: iso_c_binding
    implicit none
    private

    !--------------------------------------------------------------------------
    ! Public interface
    !--------------------------------------------------------------------------

    ! Main context type
    public :: marabunta_context

    ! Error codes
    public :: MARABUNTA_OK
    public :: MARABUNTA_ERR_NULL_CONTEXT
    public :: MARABUNTA_ERR_NULL_POINTER
    public :: MARABUNTA_ERR_INVALID_UTF8
    public :: MARABUNTA_ERR_IO
    public :: MARABUNTA_ERR_CHECKPOINT_NOT_FOUND
    public :: MARABUNTA_ERR_BUFFER_TOO_SMALL
    public :: MARABUNTA_ERR_ALREADY_INITIALIZED
    public :: MARABUNTA_ERR_NOT_INITIALIZED
    public :: MARABUNTA_ERR_ABORTED
    public :: MARABUNTA_ERR_INVALID_ARGUMENT
    public :: MARABUNTA_ERR_ALLOCATION_FAILED
    public :: MARABUNTA_ERR_TIMEOUT
    public :: MARABUNTA_ERR_PANIC

    ! Yield results
    public :: MARABUNTA_YIELD_CONTINUE
    public :: MARABUNTA_YIELD_PAUSE
    public :: MARABUNTA_YIELD_ABORT

    ! Log levels
    public :: MARABUNTA_LOG_DEBUG
    public :: MARABUNTA_LOG_INFO
    public :: MARABUNTA_LOG_WARN
    public :: MARABUNTA_LOG_ERROR

    ! Utility functions
    public :: marabunta_get_error_message
    public :: marabunta_get_version

    ! Legacy F77-style subroutines
    public :: marabunta_init
    public :: marabunta_init_ex
    public :: marabunta_finalize
    public :: marabunta_progress
    public :: marabunta_progress_increment
    public :: marabunta_progress_get
    public :: marabunta_stage
    public :: marabunta_message
    public :: marabunta_yield
    public :: marabunta_should_pause
    public :: marabunta_should_abort
    public :: marabunta_save_checkpoint
    public :: marabunta_load_checkpoint
    public :: marabunta_checkpoint_exists
    public :: marabunta_set_result
    public :: marabunta_set_result_int
    public :: marabunta_set_result_real
    public :: marabunta_set_result_string
    public :: marabunta_set_result_json
    public :: marabunta_result_append
    public :: marabunta_result_size
    public :: marabunta_log_message
    public :: marabunta_get_task_id
    public :: marabunta_get_worker_rank
    public :: marabunta_get_worker_count
    public :: marabunta_hint_memory
    public :: marabunta_hint_time
    public :: marabunta_hint_split

    !--------------------------------------------------------------------------
    ! Error codes (matching C ABI)
    !--------------------------------------------------------------------------

    integer(c_int), parameter :: MARABUNTA_OK                     = 0
    integer(c_int), parameter :: MARABUNTA_ERR_NULL_CONTEXT       = -1
    integer(c_int), parameter :: MARABUNTA_ERR_NULL_POINTER       = -2
    integer(c_int), parameter :: MARABUNTA_ERR_INVALID_UTF8       = -3
    integer(c_int), parameter :: MARABUNTA_ERR_IO                 = -4
    integer(c_int), parameter :: MARABUNTA_ERR_CHECKPOINT_NOT_FOUND = -5
    integer(c_int), parameter :: MARABUNTA_ERR_BUFFER_TOO_SMALL   = -6
    integer(c_int), parameter :: MARABUNTA_ERR_ALREADY_INITIALIZED = -7
    integer(c_int), parameter :: MARABUNTA_ERR_NOT_INITIALIZED    = -8
    integer(c_int), parameter :: MARABUNTA_ERR_ABORTED            = -9
    integer(c_int), parameter :: MARABUNTA_ERR_INVALID_ARGUMENT   = -10
    integer(c_int), parameter :: MARABUNTA_ERR_ALLOCATION_FAILED  = -11
    integer(c_int), parameter :: MARABUNTA_ERR_TIMEOUT            = -12
    integer(c_int), parameter :: MARABUNTA_ERR_PANIC              = -99

    !--------------------------------------------------------------------------
    ! Yield results
    !--------------------------------------------------------------------------

    integer(c_int), parameter :: MARABUNTA_YIELD_CONTINUE = 0
    integer(c_int), parameter :: MARABUNTA_YIELD_PAUSE    = 1
    integer(c_int), parameter :: MARABUNTA_YIELD_ABORT    = 2

    !--------------------------------------------------------------------------
    ! Log levels
    !--------------------------------------------------------------------------

    integer(c_int), parameter :: MARABUNTA_LOG_DEBUG = 0
    integer(c_int), parameter :: MARABUNTA_LOG_INFO  = 1
    integer(c_int), parameter :: MARABUNTA_LOG_WARN  = 2
    integer(c_int), parameter :: MARABUNTA_LOG_ERROR = 3

    !--------------------------------------------------------------------------
    ! Marabunta context type with type-bound procedures
    !--------------------------------------------------------------------------

    type :: marabunta_context
        type(c_ptr) :: handle = c_null_ptr
    contains
        ! Lifecycle
        procedure :: init => marabunta_context_init
        procedure :: init_ex => marabunta_context_init_ex
        procedure :: cleanup => marabunta_context_cleanup
        procedure :: is_valid => marabunta_context_is_valid

        ! Progress reporting
        procedure :: set_progress => marabunta_context_set_progress
        procedure :: increment_progress => marabunta_context_increment_progress
        procedure :: get_progress => marabunta_context_get_progress
        procedure :: set_stage => marabunta_context_set_stage
        procedure :: set_message => marabunta_context_set_message
        procedure :: hint_time => marabunta_context_hint_time
        procedure :: hint_memory => marabunta_context_hint_memory
        procedure :: hint_split => marabunta_context_hint_split

        ! Yield control
        procedure :: yield => marabunta_context_yield
        procedure :: should_pause => marabunta_context_should_pause
        procedure :: should_abort => marabunta_context_should_abort

        ! Checkpointing (matches main C API)
        procedure :: save_checkpoint => marabunta_context_save_checkpoint
        procedure :: load_checkpoint => marabunta_context_load_checkpoint
        procedure :: checkpoint_exists => marabunta_context_checkpoint_exists

        ! Convenience checkpoint methods for common types
        procedure :: save_checkpoint_int => marabunta_context_save_checkpoint_int
        procedure :: load_checkpoint_int => marabunta_context_load_checkpoint_int
        procedure :: save_checkpoint_real => marabunta_context_save_checkpoint_real
        procedure :: load_checkpoint_real => marabunta_context_load_checkpoint_real
        procedure :: save_checkpoint_array => marabunta_context_save_checkpoint_array
        procedure :: load_checkpoint_array => marabunta_context_load_checkpoint_array

        ! Results
        procedure :: set_result => marabunta_context_set_result
        procedure :: set_result_string => marabunta_context_set_result_string
        procedure :: set_result_json => marabunta_context_set_result_json
        procedure :: set_result_int => marabunta_context_set_result_int
        procedure :: set_result_real => marabunta_context_set_result_real
        procedure :: append_result => marabunta_context_append_result
        procedure :: get_result_size => marabunta_context_get_result_size

        ! Logging
        procedure :: log => marabunta_context_log
        procedure :: log_debug => marabunta_context_log_debug
        procedure :: log_info => marabunta_context_log_info
        procedure :: log_warn => marabunta_context_log_warn
        procedure :: log_error => marabunta_context_log_error

        ! Task info
        procedure :: get_task_id => marabunta_context_get_task_id
        procedure :: get_worker_rank => marabunta_context_get_worker_rank
        procedure :: get_worker_count => marabunta_context_get_worker_count
    end type marabunta_context

    !--------------------------------------------------------------------------
    ! C function interfaces (matching main include/marabunta_sdk.h)
    !--------------------------------------------------------------------------

    interface
        !----------------------------------------------------------------------
        ! Lifecycle Functions
        !----------------------------------------------------------------------

        function c_marabunta_task_init() bind(C, name='marabunta_task_init')
            import :: c_ptr
            type(c_ptr) :: c_marabunta_task_init
        end function

        function c_marabunta_task_init_ex(task_id, worker_rank, worker_count, checkpoint_dir) &
                bind(C, name='marabunta_task_init_ex')
            import :: c_ptr, c_int64_t, c_int32_t, c_char
            integer(c_int64_t), value :: task_id
            integer(c_int32_t), value :: worker_rank
            integer(c_int32_t), value :: worker_count
            character(kind=c_char), dimension(*), intent(in) :: checkpoint_dir
            type(c_ptr) :: c_marabunta_task_init_ex
        end function

        subroutine c_marabunta_task_cleanup(ctx) bind(C, name='marabunta_task_cleanup')
            import :: c_ptr
            type(c_ptr), value :: ctx
        end subroutine

        !----------------------------------------------------------------------
        ! Progress Reporting
        !----------------------------------------------------------------------

        function c_marabunta_progress_set(ctx, percent) bind(C, name='marabunta_progress_set')
            import :: c_ptr, c_int32_t, c_int8_t
            type(c_ptr), value :: ctx
            integer(c_int8_t), value :: percent
            integer(c_int32_t) :: c_marabunta_progress_set
        end function

        function c_marabunta_progress_set_stage(ctx, stage) bind(C, name='marabunta_progress_set_stage')
            import :: c_ptr, c_int32_t, c_char
            type(c_ptr), value :: ctx
            character(kind=c_char), dimension(*), intent(in) :: stage
            integer(c_int32_t) :: c_marabunta_progress_set_stage
        end function

        function c_marabunta_progress_set_message(ctx, message) bind(C, name='marabunta_progress_set_message')
            import :: c_ptr, c_int32_t, c_char
            type(c_ptr), value :: ctx
            character(kind=c_char), dimension(*), intent(in) :: message
            integer(c_int32_t) :: c_marabunta_progress_set_message
        end function

        function c_marabunta_progress_increment(ctx, delta) bind(C, name='marabunta_progress_increment')
            import :: c_ptr, c_int32_t, c_int8_t
            type(c_ptr), value :: ctx
            integer(c_int8_t), value :: delta
            integer(c_int32_t) :: c_marabunta_progress_increment
        end function

        function c_marabunta_progress_get(ctx) bind(C, name='marabunta_progress_get')
            import :: c_ptr, c_int32_t
            type(c_ptr), value :: ctx
            integer(c_int32_t) :: c_marabunta_progress_get
        end function

        !----------------------------------------------------------------------
        ! Checkpointing (Fault Tolerance)
        !----------------------------------------------------------------------

        function c_marabunta_checkpoint_save(ctx, data, len) bind(C, name='marabunta_checkpoint_save')
            import :: c_ptr, c_int32_t, c_size_t
            type(c_ptr), value :: ctx
            type(c_ptr), value :: data
            integer(c_size_t), value :: len
            integer(c_int32_t) :: c_marabunta_checkpoint_save
        end function

        function c_marabunta_checkpoint_load(ctx, buffer, max_len) bind(C, name='marabunta_checkpoint_load')
            import :: c_ptr, c_int64_t, c_size_t
            type(c_ptr), value :: ctx
            type(c_ptr), value :: buffer
            integer(c_size_t), value :: max_len
            integer(c_int64_t) :: c_marabunta_checkpoint_load
        end function

        function c_marabunta_checkpoint_exists(ctx) bind(C, name='marabunta_checkpoint_exists')
            import :: c_ptr, c_int32_t
            type(c_ptr), value :: ctx
            integer(c_int32_t) :: c_marabunta_checkpoint_exists
        end function

        !----------------------------------------------------------------------
        ! Yield Points (Cooperative Scheduling)
        !----------------------------------------------------------------------

        function c_marabunta_yield(ctx) bind(C, name='marabunta_yield')
            import :: c_ptr, c_int32_t
            type(c_ptr), value :: ctx
            integer(c_int32_t) :: c_marabunta_yield
        end function

        function c_marabunta_should_pause(ctx) bind(C, name='marabunta_should_pause')
            import :: c_ptr, c_int32_t
            type(c_ptr), value :: ctx
            integer(c_int32_t) :: c_marabunta_should_pause
        end function

        function c_marabunta_should_abort(ctx) bind(C, name='marabunta_should_abort')
            import :: c_ptr, c_int32_t
            type(c_ptr), value :: ctx
            integer(c_int32_t) :: c_marabunta_should_abort
        end function

        !----------------------------------------------------------------------
        ! Resource Hints
        !----------------------------------------------------------------------

        function c_marabunta_hint_memory_needed(ctx, bytes) bind(C, name='marabunta_hint_memory_needed')
            import :: c_ptr, c_int32_t, c_int64_t
            type(c_ptr), value :: ctx
            integer(c_int64_t), value :: bytes
            integer(c_int32_t) :: c_marabunta_hint_memory_needed
        end function

        function c_marabunta_hint_time_estimate(ctx, seconds) bind(C, name='marabunta_hint_time_estimate')
            import :: c_ptr, c_int32_t
            type(c_ptr), value :: ctx
            integer(c_int32_t), value :: seconds
            integer(c_int32_t) :: c_marabunta_hint_time_estimate
        end function

        function c_marabunta_hint_can_split(ctx, min_chunks, max_chunks) bind(C, name='marabunta_hint_can_split')
            import :: c_ptr, c_int32_t
            type(c_ptr), value :: ctx
            integer(c_int32_t), value :: min_chunks
            integer(c_int32_t), value :: max_chunks
            integer(c_int32_t) :: c_marabunta_hint_can_split
        end function

        !----------------------------------------------------------------------
        ! Communication
        !----------------------------------------------------------------------

        function c_marabunta_get_task_id(ctx) bind(C, name='marabunta_get_task_id')
            import :: c_ptr, c_int64_t
            type(c_ptr), value :: ctx
            integer(c_int64_t) :: c_marabunta_get_task_id
        end function

        function c_marabunta_get_worker_count(ctx) bind(C, name='marabunta_get_worker_count')
            import :: c_ptr, c_int32_t
            type(c_ptr), value :: ctx
            integer(c_int32_t) :: c_marabunta_get_worker_count
        end function

        function c_marabunta_get_worker_rank(ctx) bind(C, name='marabunta_get_worker_rank')
            import :: c_ptr, c_int32_t
            type(c_ptr), value :: ctx
            integer(c_int32_t) :: c_marabunta_get_worker_rank
        end function

        !----------------------------------------------------------------------
        ! Logging
        !----------------------------------------------------------------------

        function c_marabunta_log(ctx, level, message) bind(C, name='marabunta_log')
            import :: c_ptr, c_int32_t, c_char
            type(c_ptr), value :: ctx
            integer(c_int32_t), value :: level
            character(kind=c_char), dimension(*), intent(in) :: message
            integer(c_int32_t) :: c_marabunta_log
        end function

        function c_marabunta_log_debug(ctx, message) bind(C, name='marabunta_log_debug')
            import :: c_ptr, c_int32_t, c_char
            type(c_ptr), value :: ctx
            character(kind=c_char), dimension(*), intent(in) :: message
            integer(c_int32_t) :: c_marabunta_log_debug
        end function

        function c_marabunta_log_info(ctx, message) bind(C, name='marabunta_log_info')
            import :: c_ptr, c_int32_t, c_char
            type(c_ptr), value :: ctx
            character(kind=c_char), dimension(*), intent(in) :: message
            integer(c_int32_t) :: c_marabunta_log_info
        end function

        function c_marabunta_log_warn(ctx, message) bind(C, name='marabunta_log_warn')
            import :: c_ptr, c_int32_t, c_char
            type(c_ptr), value :: ctx
            character(kind=c_char), dimension(*), intent(in) :: message
            integer(c_int32_t) :: c_marabunta_log_warn
        end function

        function c_marabunta_log_error(ctx, message) bind(C, name='marabunta_log_error')
            import :: c_ptr, c_int32_t, c_char
            type(c_ptr), value :: ctx
            character(kind=c_char), dimension(*), intent(in) :: message
            integer(c_int32_t) :: c_marabunta_log_error
        end function

        !----------------------------------------------------------------------
        ! Results
        !----------------------------------------------------------------------

        function c_marabunta_result_set(ctx, data, len) bind(C, name='marabunta_result_set')
            import :: c_ptr, c_int32_t, c_size_t
            type(c_ptr), value :: ctx
            type(c_ptr), value :: data
            integer(c_size_t), value :: len
            integer(c_int32_t) :: c_marabunta_result_set
        end function

        function c_marabunta_result_set_json(ctx, json_string) bind(C, name='marabunta_result_set_json')
            import :: c_ptr, c_int32_t, c_char
            type(c_ptr), value :: ctx
            character(kind=c_char), dimension(*), intent(in) :: json_string
            integer(c_int32_t) :: c_marabunta_result_set_json
        end function

        function c_marabunta_result_append(ctx, data, len) bind(C, name='marabunta_result_append')
            import :: c_ptr, c_int32_t, c_size_t
            type(c_ptr), value :: ctx
            type(c_ptr), value :: data
            integer(c_size_t), value :: len
            integer(c_int32_t) :: c_marabunta_result_append
        end function

        function c_marabunta_result_size(ctx) bind(C, name='marabunta_result_size')
            import :: c_ptr, c_int64_t
            type(c_ptr), value :: ctx
            integer(c_int64_t) :: c_marabunta_result_size
        end function

        function c_marabunta_result_get(ctx, data, max_len) bind(C, name='marabunta_result_get')
            import :: c_ptr, c_int64_t, c_size_t
            type(c_ptr), value :: ctx
            type(c_ptr), value :: data
            integer(c_size_t), value :: max_len
            integer(c_int64_t) :: c_marabunta_result_get
        end function

        !----------------------------------------------------------------------
        ! Utility
        !----------------------------------------------------------------------

        function c_marabunta_error_message(code) bind(C, name='marabunta_error_message')
            import :: c_ptr, c_int32_t
            integer(c_int32_t), value :: code
            type(c_ptr) :: c_marabunta_error_message
        end function
    end interface

    ! Module-level context for legacy F77 interface
    type(marabunta_context), save :: global_context

contains

    !--------------------------------------------------------------------------
    ! String conversion utilities
    !--------------------------------------------------------------------------

    !> Convert Fortran string to C string (null-terminated)
    function f_to_c_string(f_string) result(c_string)
        character(len=*), intent(in) :: f_string
        character(len=len_trim(f_string)+1, kind=c_char) :: c_string

        c_string = trim(f_string) // c_null_char
    end function f_to_c_string

    !> Convert C string pointer to Fortran string
    function c_to_f_string(c_ptr_str) result(f_string)
        type(c_ptr), intent(in) :: c_ptr_str
        character(len=:), allocatable :: f_string
        character(kind=c_char), pointer :: c_chars(:)
        integer :: i, length

        if (.not. c_associated(c_ptr_str)) then
            f_string = ''
            return
        end if

        ! Find string length (scan for null terminator)
        length = 0
        call c_f_pointer(c_ptr_str, c_chars, [4096])  ! Max scan length
        do i = 1, 4096
            if (c_chars(i) == c_null_char) exit
            length = length + 1
        end do

        ! Allocate and copy
        allocate(character(len=length) :: f_string)
        do i = 1, length
            f_string(i:i) = c_chars(i)
        end do
    end function c_to_f_string

    !--------------------------------------------------------------------------
    ! Type-bound procedures for marabunta_context
    !--------------------------------------------------------------------------

    !> Initialize the task context with default settings
    subroutine marabunta_context_init(this, ierr)
        class(marabunta_context), intent(inout) :: this
        integer, intent(out), optional :: ierr

        this%handle = c_marabunta_task_init()

        if (present(ierr)) then
            if (c_associated(this%handle)) then
                ierr = 0
            else
                ierr = MARABUNTA_ERR_ALLOCATION_FAILED
            end if
        end if
    end subroutine marabunta_context_init

    !> Initialize the task context with specific parameters
    subroutine marabunta_context_init_ex(this, task_id, worker_rank, worker_count, &
                                     checkpoint_dir, ierr)
        class(marabunta_context), intent(inout) :: this
        integer(8), intent(in) :: task_id
        integer, intent(in) :: worker_rank
        integer, intent(in) :: worker_count
        character(len=*), intent(in), optional :: checkpoint_dir
        integer, intent(out), optional :: ierr
        character(len=256, kind=c_char) :: c_dir

        if (present(checkpoint_dir)) then
            c_dir = f_to_c_string(checkpoint_dir)
        else
            c_dir = c_null_char
        end if

        this%handle = c_marabunta_task_init_ex( &
            int(task_id, c_int64_t), &
            int(worker_rank, c_int32_t), &
            int(worker_count, c_int32_t), &
            c_dir)

        if (present(ierr)) then
            if (c_associated(this%handle)) then
                ierr = 0
            else
                ierr = MARABUNTA_ERR_ALLOCATION_FAILED
            end if
        end if
    end subroutine marabunta_context_init_ex

    !> Clean up the task context
    subroutine marabunta_context_cleanup(this)
        class(marabunta_context), intent(inout) :: this

        if (c_associated(this%handle)) then
            call c_marabunta_task_cleanup(this%handle)
            this%handle = c_null_ptr
        end if
    end subroutine marabunta_context_cleanup

    !> Check if context is valid
    function marabunta_context_is_valid(this) result(valid)
        class(marabunta_context), intent(in) :: this
        logical :: valid

        valid = c_associated(this%handle)
    end function marabunta_context_is_valid

    !> Set progress percentage (0-100)
    subroutine marabunta_context_set_progress(this, percent, ierr)
        class(marabunta_context), intent(inout) :: this
        integer, intent(in) :: percent
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc

        rc = c_marabunta_progress_set(this%handle, int(percent, c_int8_t))
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_set_progress

    !> Increment progress by a delta amount
    subroutine marabunta_context_increment_progress(this, delta, ierr)
        class(marabunta_context), intent(inout) :: this
        integer, intent(in) :: delta
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc

        rc = c_marabunta_progress_increment(this%handle, int(delta, c_int8_t))
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_increment_progress

    !> Get current progress percentage
    function marabunta_context_get_progress(this) result(percent)
        class(marabunta_context), intent(inout) :: this
        integer :: percent
        integer(c_int32_t) :: rc

        rc = c_marabunta_progress_get(this%handle)
        percent = int(rc)
    end function marabunta_context_get_progress

    !> Set current stage name
    subroutine marabunta_context_set_stage(this, stage_name, ierr)
        class(marabunta_context), intent(inout) :: this
        character(len=*), intent(in) :: stage_name
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=len_trim(stage_name)+1, kind=c_char) :: c_str

        c_str = f_to_c_string(stage_name)
        rc = c_marabunta_progress_set_stage(this%handle, c_str)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_set_stage

    !> Set progress message
    subroutine marabunta_context_set_message(this, message, ierr)
        class(marabunta_context), intent(inout) :: this
        character(len=*), intent(in) :: message
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=len_trim(message)+1, kind=c_char) :: c_msg

        c_msg = f_to_c_string(message)
        rc = c_marabunta_progress_set_message(this%handle, c_msg)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_set_message

    !> Hint expected runtime in seconds
    subroutine marabunta_context_hint_time(this, seconds, ierr)
        class(marabunta_context), intent(inout) :: this
        integer, intent(in) :: seconds
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc

        rc = c_marabunta_hint_time_estimate(this%handle, int(seconds, c_int32_t))
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_hint_time

    !> Hint memory requirements in bytes
    subroutine marabunta_context_hint_memory(this, bytes, ierr)
        class(marabunta_context), intent(inout) :: this
        integer(8), intent(in) :: bytes
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc

        rc = c_marabunta_hint_memory_needed(this%handle, int(bytes, c_int64_t))
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_hint_memory

    !> Hint that this task can be split into parallel chunks
    subroutine marabunta_context_hint_split(this, min_chunks, max_chunks, ierr)
        class(marabunta_context), intent(inout) :: this
        integer, intent(in) :: min_chunks
        integer, intent(in) :: max_chunks
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc

        rc = c_marabunta_hint_can_split(this%handle, &
            int(min_chunks, c_int32_t), int(max_chunks, c_int32_t))
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_hint_split

    !> Yield point - check for pause/abort
    subroutine marabunta_context_yield(this, result, ierr)
        class(marabunta_context), intent(inout) :: this
        integer, intent(out) :: result
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc

        rc = c_marabunta_yield(this%handle)
        result = int(rc)
        if (present(ierr)) ierr = MARABUNTA_OK
    end subroutine marabunta_context_yield

    !> Check if task should pause
    function marabunta_context_should_pause(this) result(should)
        class(marabunta_context), intent(inout) :: this
        logical :: should
        integer(c_int32_t) :: rc

        rc = c_marabunta_should_pause(this%handle)
        should = (rc == 1)
    end function marabunta_context_should_pause

    !> Check if task should abort
    function marabunta_context_should_abort(this) result(should)
        class(marabunta_context), intent(inout) :: this
        logical :: should
        integer(c_int32_t) :: rc

        rc = c_marabunta_should_abort(this%handle)
        should = (rc == 1)
    end function marabunta_context_should_abort

    !> Save binary checkpoint data (matches main C API)
    subroutine marabunta_context_save_checkpoint(this, data, size, ierr)
        class(marabunta_context), intent(inout) :: this
        type(c_ptr), intent(in) :: data
        integer(c_size_t), intent(in) :: size
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc

        rc = c_marabunta_checkpoint_save(this%handle, data, size)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_save_checkpoint

    !> Load binary checkpoint data (matches main C API)
    subroutine marabunta_context_load_checkpoint(this, buffer, buffer_size, bytes_read, ierr)
        class(marabunta_context), intent(inout) :: this
        type(c_ptr), intent(in) :: buffer
        integer(c_size_t), intent(in) :: buffer_size
        integer(8), intent(out) :: bytes_read
        integer, intent(out), optional :: ierr
        integer(c_int64_t) :: rc

        rc = c_marabunta_checkpoint_load(this%handle, buffer, buffer_size)
        if (rc >= 0) then
            bytes_read = int(rc, 8)
            if (present(ierr)) ierr = MARABUNTA_OK
        else
            bytes_read = 0
            if (present(ierr)) ierr = int(rc)
        end if
    end subroutine marabunta_context_load_checkpoint

    !> Check if checkpoint exists (matches main C API)
    function marabunta_context_checkpoint_exists(this) result(exists)
        class(marabunta_context), intent(inout) :: this
        logical :: exists
        integer(c_int32_t) :: rc

        rc = c_marabunta_checkpoint_exists(this%handle)
        exists = (rc == 1)
    end function marabunta_context_checkpoint_exists

    !> Save integer checkpoint
    subroutine marabunta_context_save_checkpoint_int(this, value, ierr)
        class(marabunta_context), intent(inout) :: this
        integer, intent(in), target :: value
        integer, intent(out), optional :: ierr

        call this%save_checkpoint(c_loc(value), int(c_sizeof(value), c_size_t), ierr)
    end subroutine marabunta_context_save_checkpoint_int

    !> Load integer checkpoint
    subroutine marabunta_context_load_checkpoint_int(this, value, ierr)
        class(marabunta_context), intent(inout) :: this
        integer, intent(out), target :: value
        integer, intent(out), optional :: ierr
        integer(8) :: bytes_read

        call this%load_checkpoint(c_loc(value), int(c_sizeof(value), c_size_t), bytes_read, ierr)
    end subroutine marabunta_context_load_checkpoint_int

    !> Save real(8) checkpoint
    subroutine marabunta_context_save_checkpoint_real(this, value, ierr)
        class(marabunta_context), intent(inout) :: this
        real(8), intent(in), target :: value
        integer, intent(out), optional :: ierr

        call this%save_checkpoint(c_loc(value), int(c_sizeof(value), c_size_t), ierr)
    end subroutine marabunta_context_save_checkpoint_real

    !> Load real(8) checkpoint
    subroutine marabunta_context_load_checkpoint_real(this, value, ierr)
        class(marabunta_context), intent(inout) :: this
        real(8), intent(out), target :: value
        integer, intent(out), optional :: ierr
        integer(8) :: bytes_read

        call this%load_checkpoint(c_loc(value), int(c_sizeof(value), c_size_t), bytes_read, ierr)
    end subroutine marabunta_context_load_checkpoint_real

    !> Save array checkpoint
    subroutine marabunta_context_save_checkpoint_array(this, array, ierr)
        class(marabunta_context), intent(inout) :: this
        real(8), dimension(:), intent(in), target :: array
        integer, intent(out), optional :: ierr
        integer(c_size_t) :: array_size

        array_size = int(size(array) * c_sizeof(array(1)), c_size_t)
        call this%save_checkpoint(c_loc(array), array_size, ierr)
    end subroutine marabunta_context_save_checkpoint_array

    !> Load array checkpoint
    subroutine marabunta_context_load_checkpoint_array(this, array, ierr)
        class(marabunta_context), intent(inout) :: this
        real(8), dimension(:), intent(out), target :: array
        integer, intent(out), optional :: ierr
        integer(c_size_t) :: buffer_size
        integer(8) :: bytes_read

        buffer_size = int(size(array) * c_sizeof(array(1)), c_size_t)
        call this%load_checkpoint(c_loc(array), buffer_size, bytes_read, ierr)
    end subroutine marabunta_context_load_checkpoint_array

    !> Set binary result
    subroutine marabunta_context_set_result(this, data, size, ierr)
        class(marabunta_context), intent(inout) :: this
        type(c_ptr), intent(in) :: data
        integer(c_size_t), intent(in) :: size
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc

        rc = c_marabunta_result_set(this%handle, data, size)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_set_result

    !> Set string result (via JSON)
    subroutine marabunta_context_set_result_string(this, value, ierr)
        class(marabunta_context), intent(inout) :: this
        character(len=*), intent(in) :: value
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=len_trim(value)+3, kind=c_char) :: json_value

        ! Wrap string in JSON quotes
        json_value = '"' // trim(value) // '"' // c_null_char
        rc = c_marabunta_result_set_json(this%handle, json_value)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_set_result_string

    !> Set JSON result directly
    subroutine marabunta_context_set_result_json(this, json_string, ierr)
        class(marabunta_context), intent(inout) :: this
        character(len=*), intent(in) :: json_string
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=len_trim(json_string)+1, kind=c_char) :: c_value

        c_value = f_to_c_string(json_string)
        rc = c_marabunta_result_set_json(this%handle, c_value)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_set_result_json

    !> Set integer result (as JSON number)
    subroutine marabunta_context_set_result_int(this, value, ierr)
        class(marabunta_context), intent(inout) :: this
        integer(8), intent(in) :: value
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=32, kind=c_char) :: json_value

        write(json_value, '(I0,A)') value, c_null_char
        rc = c_marabunta_result_set_json(this%handle, json_value)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_set_result_int

    !> Set real(8) result (as JSON number)
    subroutine marabunta_context_set_result_real(this, value, ierr)
        class(marabunta_context), intent(inout) :: this
        real(8), intent(in) :: value
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=32, kind=c_char) :: json_value

        write(json_value, '(ES22.15,A)') value, c_null_char
        rc = c_marabunta_result_set_json(this%handle, json_value)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_set_result_real

    !> Append data to result
    subroutine marabunta_context_append_result(this, data, size, ierr)
        class(marabunta_context), intent(inout) :: this
        type(c_ptr), intent(in) :: data
        integer(c_size_t), intent(in) :: size
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc

        rc = c_marabunta_result_append(this%handle, data, size)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_append_result

    !> Get current result size
    function marabunta_context_get_result_size(this) result(size)
        class(marabunta_context), intent(inout) :: this
        integer(8) :: size
        integer(c_int64_t) :: rc

        rc = c_marabunta_result_size(this%handle)
        size = int(rc, 8)
    end function marabunta_context_get_result_size

    !> Log message at specified level
    subroutine marabunta_context_log(this, level, message, ierr)
        class(marabunta_context), intent(inout) :: this
        integer, intent(in) :: level
        character(len=*), intent(in) :: message
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=len_trim(message)+1, kind=c_char) :: c_msg

        c_msg = f_to_c_string(message)
        rc = c_marabunta_log(this%handle, int(level, c_int32_t), c_msg)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_log

    !> Log debug message
    subroutine marabunta_context_log_debug(this, message, ierr)
        class(marabunta_context), intent(inout) :: this
        character(len=*), intent(in) :: message
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=len_trim(message)+1, kind=c_char) :: c_msg

        c_msg = f_to_c_string(message)
        rc = c_marabunta_log_debug(this%handle, c_msg)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_log_debug

    !> Log info message
    subroutine marabunta_context_log_info(this, message, ierr)
        class(marabunta_context), intent(inout) :: this
        character(len=*), intent(in) :: message
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=len_trim(message)+1, kind=c_char) :: c_msg

        c_msg = f_to_c_string(message)
        rc = c_marabunta_log_info(this%handle, c_msg)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_log_info

    !> Log warning message
    subroutine marabunta_context_log_warn(this, message, ierr)
        class(marabunta_context), intent(inout) :: this
        character(len=*), intent(in) :: message
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=len_trim(message)+1, kind=c_char) :: c_msg

        c_msg = f_to_c_string(message)
        rc = c_marabunta_log_warn(this%handle, c_msg)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_log_warn

    !> Log error message
    subroutine marabunta_context_log_error(this, message, ierr)
        class(marabunta_context), intent(inout) :: this
        character(len=*), intent(in) :: message
        integer, intent(out), optional :: ierr
        integer(c_int32_t) :: rc
        character(len=len_trim(message)+1, kind=c_char) :: c_msg

        c_msg = f_to_c_string(message)
        rc = c_marabunta_log_error(this%handle, c_msg)
        if (present(ierr)) ierr = int(rc)
    end subroutine marabunta_context_log_error

    !> Get task ID
    function marabunta_context_get_task_id(this) result(task_id)
        class(marabunta_context), intent(inout) :: this
        integer(8) :: task_id

        task_id = int(c_marabunta_get_task_id(this%handle), 8)
    end function marabunta_context_get_task_id

    !> Get worker rank
    function marabunta_context_get_worker_rank(this) result(rank)
        class(marabunta_context), intent(inout) :: this
        integer :: rank

        rank = int(c_marabunta_get_worker_rank(this%handle))
    end function marabunta_context_get_worker_rank

    !> Get total worker count
    function marabunta_context_get_worker_count(this) result(count)
        class(marabunta_context), intent(inout) :: this
        integer :: count

        count = int(c_marabunta_get_worker_count(this%handle))
    end function marabunta_context_get_worker_count

    !--------------------------------------------------------------------------
    ! Module-level utility functions
    !--------------------------------------------------------------------------

    !> Get human-readable error message
    function marabunta_get_error_message(code) result(message)
        integer, intent(in) :: code
        character(len=:), allocatable :: message
        type(c_ptr) :: c_msg

        c_msg = c_marabunta_error_message(int(code, c_int32_t))
        message = c_to_f_string(c_msg)
    end function marabunta_get_error_message

    !> Get SDK version string (static fallback since C API may not have this)
    function marabunta_get_version() result(version)
        character(len=:), allocatable :: version

        version = '1.0.0'
    end function marabunta_get_version

    !--------------------------------------------------------------------------
    ! Legacy F77-style subroutines (using global context)
    !--------------------------------------------------------------------------

    !> Initialize global context (F77 style)
    subroutine marabunta_init(ierr)
        integer, intent(out), optional :: ierr

        call global_context%init(ierr)
    end subroutine marabunta_init

    !> Initialize global context with parameters (F77 style)
    subroutine marabunta_init_ex(task_id, worker_rank, worker_count, checkpoint_dir, ierr)
        integer(8), intent(in) :: task_id
        integer, intent(in) :: worker_rank
        integer, intent(in) :: worker_count
        character(len=*), intent(in), optional :: checkpoint_dir
        integer, intent(out), optional :: ierr

        call global_context%init_ex(task_id, worker_rank, worker_count, checkpoint_dir, ierr)
    end subroutine marabunta_init_ex

    !> Finalize global context (F77 style)
    subroutine marabunta_finalize()
        call global_context%cleanup()
    end subroutine marabunta_finalize

    !> Set progress (F77 style)
    subroutine marabunta_progress(percent, ierr)
        integer, intent(in) :: percent
        integer, intent(out), optional :: ierr

        call global_context%set_progress(percent, ierr)
    end subroutine marabunta_progress

    !> Increment progress (F77 style)
    subroutine marabunta_progress_increment(delta, ierr)
        integer, intent(in) :: delta
        integer, intent(out), optional :: ierr

        call global_context%increment_progress(delta, ierr)
    end subroutine marabunta_progress_increment

    !> Get progress (F77 style)
    function marabunta_progress_get() result(percent)
        integer :: percent

        percent = global_context%get_progress()
    end function marabunta_progress_get

    !> Set stage name (F77 style)
    subroutine marabunta_stage(stage_name, ierr)
        character(len=*), intent(in) :: stage_name
        integer, intent(out), optional :: ierr

        call global_context%set_stage(stage_name, ierr)
    end subroutine marabunta_stage

    !> Set message (F77 style)
    subroutine marabunta_message(message, ierr)
        character(len=*), intent(in) :: message
        integer, intent(out), optional :: ierr

        call global_context%set_message(message, ierr)
    end subroutine marabunta_message

    !> Yield point (F77 style)
    subroutine marabunta_yield(result, ierr)
        integer, intent(out) :: result
        integer, intent(out), optional :: ierr

        call global_context%yield(result, ierr)
    end subroutine marabunta_yield

    !> Check if should pause (F77 style)
    function marabunta_should_pause() result(should)
        logical :: should

        should = global_context%should_pause()
    end function marabunta_should_pause

    !> Check if should abort (F77 style)
    function marabunta_should_abort() result(should)
        logical :: should

        should = global_context%should_abort()
    end function marabunta_should_abort

    !> Save checkpoint (F77 style)
    subroutine marabunta_save_checkpoint(data, size, ierr)
        type(c_ptr), intent(in) :: data
        integer(c_size_t), intent(in) :: size
        integer, intent(out), optional :: ierr

        call global_context%save_checkpoint(data, size, ierr)
    end subroutine marabunta_save_checkpoint

    !> Load checkpoint (F77 style)
    subroutine marabunta_load_checkpoint(buffer, buffer_size, bytes_read, ierr)
        type(c_ptr), intent(in) :: buffer
        integer(c_size_t), intent(in) :: buffer_size
        integer(8), intent(out) :: bytes_read
        integer, intent(out), optional :: ierr

        call global_context%load_checkpoint(buffer, buffer_size, bytes_read, ierr)
    end subroutine marabunta_load_checkpoint

    !> Check if checkpoint exists (F77 style)
    function marabunta_checkpoint_exists() result(exists)
        logical :: exists

        exists = global_context%checkpoint_exists()
    end function marabunta_checkpoint_exists

    !> Set binary result (F77 style)
    subroutine marabunta_set_result(data, size, ierr)
        type(c_ptr), intent(in) :: data
        integer(c_size_t), intent(in) :: size
        integer, intent(out), optional :: ierr

        call global_context%set_result(data, size, ierr)
    end subroutine marabunta_set_result

    !> Set integer result (F77 style)
    subroutine marabunta_set_result_int(value, ierr)
        integer(8), intent(in) :: value
        integer, intent(out), optional :: ierr

        call global_context%set_result_int(value, ierr)
    end subroutine marabunta_set_result_int

    !> Set real result (F77 style)
    subroutine marabunta_set_result_real(value, ierr)
        real(8), intent(in) :: value
        integer, intent(out), optional :: ierr

        call global_context%set_result_real(value, ierr)
    end subroutine marabunta_set_result_real

    !> Set string result (F77 style)
    subroutine marabunta_set_result_string(value, ierr)
        character(len=*), intent(in) :: value
        integer, intent(out), optional :: ierr

        call global_context%set_result_string(value, ierr)
    end subroutine marabunta_set_result_string

    !> Set JSON result (F77 style)
    subroutine marabunta_set_result_json(json_string, ierr)
        character(len=*), intent(in) :: json_string
        integer, intent(out), optional :: ierr

        call global_context%set_result_json(json_string, ierr)
    end subroutine marabunta_set_result_json

    !> Append to result (F77 style)
    subroutine marabunta_result_append(data, size, ierr)
        type(c_ptr), intent(in) :: data
        integer(c_size_t), intent(in) :: size
        integer, intent(out), optional :: ierr

        call global_context%append_result(data, size, ierr)
    end subroutine marabunta_result_append

    !> Get result size (F77 style)
    function marabunta_result_size() result(size)
        integer(8) :: size

        size = global_context%get_result_size()
    end function marabunta_result_size

    !> Log message (F77 style)
    subroutine marabunta_log_message(level, message, ierr)
        integer, intent(in) :: level
        character(len=*), intent(in) :: message
        integer, intent(out), optional :: ierr

        call global_context%log(level, message, ierr)
    end subroutine marabunta_log_message

    !> Get task ID (F77 style)
    function marabunta_get_task_id() result(task_id)
        integer(8) :: task_id

        task_id = global_context%get_task_id()
    end function marabunta_get_task_id

    !> Get worker rank (F77 style)
    function marabunta_get_worker_rank() result(rank)
        integer :: rank

        rank = global_context%get_worker_rank()
    end function marabunta_get_worker_rank

    !> Get worker count (F77 style)
    function marabunta_get_worker_count() result(count)
        integer :: count

        count = global_context%get_worker_count()
    end function marabunta_get_worker_count

    !> Hint memory requirement (F77 style)
    subroutine marabunta_hint_memory(bytes, ierr)
        integer(8), intent(in) :: bytes
        integer, intent(out), optional :: ierr

        call global_context%hint_memory(bytes, ierr)
    end subroutine marabunta_hint_memory

    !> Hint expected runtime (F77 style)
    subroutine marabunta_hint_time(seconds, ierr)
        integer, intent(in) :: seconds
        integer, intent(out), optional :: ierr

        call global_context%hint_time(seconds, ierr)
    end subroutine marabunta_hint_time

    !> Hint task can be split (F77 style)
    subroutine marabunta_hint_split(min_chunks, max_chunks, ierr)
        integer, intent(in) :: min_chunks
        integer, intent(in) :: max_chunks
        integer, intent(out), optional :: ierr

        call global_context%hint_split(min_chunks, max_chunks, ierr)
    end subroutine marabunta_hint_split

end module marabunta_sdk
