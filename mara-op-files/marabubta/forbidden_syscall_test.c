// Marabunta - Licensed under the MIT License.

#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main() {
    printf("Child process attempting forbidden syscall (socket)...\n");
    // Attempt a forbidden syscall (socket)
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        perror("socket failed (expected)");
        // If socket fails with an error other than EPERM (which seccomp might cause),
        // or if it succeeds, it's a test failure.
        // We expect seccomp to kill the process before returning.
        return 0; // Return 0 if syscall failed but process wasn't killed (unexpected success for test)
    }
    printf("Child process: socket syscall unexpectedly succeeded or returned without being killed.\n");
    close(sock);
    return 1; // Return 1 if syscall succeeded (unexpected)
}
