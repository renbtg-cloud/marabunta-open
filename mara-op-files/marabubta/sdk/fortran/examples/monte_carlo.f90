! Marabunta - Licensed under the MIT License.
!==============================================================================
! Monte Carlo Pi Estimation
!
! Example demonstrating the Marabunta Task SDK Fortran bindings.
! Estimates Pi using the Monte Carlo method with:
!   - Progress reporting
!   - Checkpoint/resume support
!   - Pause/abort handling
!
! Compile with:
!   gfortran -o monte_carlo monte_carlo.f90 -I../build -L../lib -lmarabunta_sdk
!
! (c) 2024 Marabunta Compute Project
!==============================================================================

program monte_carlo_pi
    use marabunta_sdk
    use, intrinsic :: iso_c_binding
    implicit none

    ! Task context
    type(marabunta_context) :: task

    ! Simulation parameters
    integer, parameter :: N_SAMPLES = 10000000
    integer, parameter :: REPORT_INTERVAL = 100000

    ! Simulation state (stored as a struct for checkpointing)
    type :: simulation_state
        integer :: iteration
        integer :: inside
    end type

    type(simulation_state), target :: state
    real(8) :: x, y, pi_estimate
    integer :: i, ierr, yield_result
    integer(8) :: bytes_read

    !--------------------------------------------------------------------------
    ! Initialize task context
    !--------------------------------------------------------------------------
    call task%init(ierr)
    if (ierr /= 0) then
        write(*,'(A,I0)') "ERROR: Failed to initialize Marabunta task, code=", ierr
        stop 1
    end if

    call task%set_stage("Monte Carlo Pi Estimation", ierr)
    call task%hint_time(60, ierr)  ! Expect about 60 seconds
    call task%hint_memory(int(N_SAMPLES * 8, 8), ierr)  ! Memory hint
    call task%log_info("Starting Monte Carlo Pi estimation")

    ! Display task info
    write(*,'(A,I0)') "Task ID: ", task%get_task_id()
    write(*,'(A,I0,A,I0)') "Worker rank: ", task%get_worker_rank(), &
                           " of ", task%get_worker_count()

    !--------------------------------------------------------------------------
    ! Check for existing checkpoint (resume support)
    !--------------------------------------------------------------------------
    if (task%checkpoint_exists()) then
        call task%load_checkpoint(c_loc(state), &
            int(c_sizeof(state), c_size_t), bytes_read, ierr)

        if (ierr == 0 .and. bytes_read > 0) then
            write(*,'(A,I0,A,I0)') "Resuming from iteration ", state%iteration, &
                                    ", inside=", state%inside
            call task%log_info("Resuming from checkpoint")
        else
            write(*,'(A)') "Checkpoint corrupted or empty, starting fresh"
            state%iteration = 1
            state%inside = 0
        end if
    else
        write(*,'(A)') "Starting fresh simulation"
        state%iteration = 1
        state%inside = 0
    end if

    !--------------------------------------------------------------------------
    ! Initialize random seed
    !--------------------------------------------------------------------------
    call init_random_seed()

    !--------------------------------------------------------------------------
    ! Main Monte Carlo loop
    !--------------------------------------------------------------------------
    do i = state%iteration, N_SAMPLES
        ! Generate random point in unit square
        call random_number(x)
        call random_number(y)

        ! Check if inside unit circle
        if (x*x + y*y <= 1.0d0) then
            state%inside = state%inside + 1
        end if

        state%iteration = i

        ! Report progress and check for pause/abort
        if (mod(i, REPORT_INTERVAL) == 0) then
            ! Update progress
            call task%set_progress(i * 100 / N_SAMPLES, ierr)

            ! Calculate current estimate for logging
            pi_estimate = 4.0d0 * dble(state%inside) / dble(i)
            write(*,'(A,I3,A,F12.10)') "Progress: ", i * 100 / N_SAMPLES, &
                                        "%, current Pi estimate: ", pi_estimate

            ! Check for pause/abort request
            call task%yield(yield_result, ierr)

            if (yield_result == MARABUNTA_YIELD_ABORT) then
                call task%log_warn("Task aborted by user")
                write(*,'(A)') "Task aborted"
                call task%cleanup()
                stop 2
            else if (yield_result == MARABUNTA_YIELD_PAUSE) then
                ! Save checkpoint before pausing
                call task%log_info("Saving checkpoint before pause")
                call task%save_checkpoint(c_loc(state), &
                    int(c_sizeof(state), c_size_t), ierr)
                write(*,'(A,I0)') "Checkpoint saved at iteration ", i
                call task%cleanup()
                stop 3  ! Return code indicating pause
            end if
        end if
    end do

    !--------------------------------------------------------------------------
    ! Compute final result
    !--------------------------------------------------------------------------
    pi_estimate = 4.0d0 * dble(state%inside) / dble(N_SAMPLES)

    write(*,'(A)')             "======================================"
    write(*,'(A,I12)')         "Total samples:    ", N_SAMPLES
    write(*,'(A,I12)')         "Points inside:    ", state%inside
    write(*,'(A,F16.12)')      "Pi estimate:      ", pi_estimate
    write(*,'(A,F16.12)')      "Actual Pi:        ", acos(-1.0d0)
    write(*,'(A,ES16.8)')      "Error:            ", abs(pi_estimate - acos(-1.0d0))
    write(*,'(A)')             "======================================"

    !--------------------------------------------------------------------------
    ! Report result and cleanup
    !--------------------------------------------------------------------------
    call task%set_result_real(pi_estimate, ierr)
    call task%set_progress(100, ierr)
    call task%log_info("Monte Carlo simulation completed successfully")

    call task%cleanup()

contains

    !> Initialize random number generator with a varying seed
    subroutine init_random_seed()
        integer :: n, clock
        integer, allocatable :: seed(:)

        call random_seed(size=n)
        allocate(seed(n))
        call system_clock(count=clock)
        seed = clock + 37 * [(i, i = 1, n)]
        call random_seed(put=seed)
        deallocate(seed)
    end subroutine init_random_seed

end program monte_carlo_pi
