// Disposable nativeidentity test process. Never installed or used by production.
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc < 2) return 2;
    int fd;
    if (strcmp(argv[1], "--inherit") == 0) {
        if (argc != 3) return 3;
        fd = atoi(argv[2]);
    } else {
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) return 4;
        struct sockaddr_un address;
        memset(&address, 0, sizeof(address));
        address.sun_family = AF_UNIX;
        if (strlen(argv[1]) >= sizeof(address.sun_path)) return 5;
        strcpy(address.sun_path, argv[1]);
        if (connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) return 6;
    }
    if (write(fd, "R", 1) != 1) return 7;
    char command;
    while (read(fd, &command, 1) == 1) {
        if (command == 'x') break;
        if (command == 'e' && argc == 3) {
            char inherited_fd[24];
            snprintf(inherited_fd, sizeof(inherited_fd), "%d", fd);
            char *args[] = { argv[2], "--inherit", inherited_fd, NULL };
            execv(argv[2], args);
            return 8;
        }
    }
    close(fd);
    return 0;
}
