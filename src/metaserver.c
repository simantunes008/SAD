#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <fcntl.h>
#include "remote.h"
#include "namespace.h"
#include "serverutils.h"

int server_fd;
Namespace* ns;
char *datanodes[MAX_DATANODES];
int datanode_count = 0;
int datanode_index = 0;
pthread_mutex_t datanode_mutex = PTHREAD_MUTEX_INITIALIZER;

void intHandler(int dummy) {
    printf("Server shutting down\n");
    close(server_fd);
    namespace_destroy(ns);
    exit(0);   
}


void handle_write(int socket_fd, MSG m){
    char path[PSIZE];
    handle_path(m.path, path);

    Filemeta meta;
    if (namespace_get(ns, path, &meta) == 0) {
        m.server_count = meta.server_count;
        for (int i = 0; i < meta.server_count; i++) {
            strcpy(m.servers[i], meta.servers[i]);
        }
        printf("File already exists, sending %d servers for update\n", meta.server_count);
    } else {
        pthread_mutex_lock(&datanode_mutex);

        int replicas = (datanode_count < MAX_REPLICAS) ? datanode_count : MAX_REPLICAS;
        m.server_count = replicas;

        for (int i = 0; i < replicas; i++) {
            strcpy(m.servers[i], datanodes[datanode_index]);
            datanode_index = (datanode_index + 1) % datanode_count;
        }

        pthread_mutex_unlock(&datanode_mutex);
        
        printf("New file, selected %d servers for write:", replicas);
        for (int i = 0; i < replicas; i++) {
            printf(" [%s]", m.servers[i]);
        }
        printf("\n");
    }

    write(socket_fd, &m, sizeof(m));
}


void handle_read(int socket_fd, MSG m){
    char path[PSIZE];
    handle_path(m.path, path);

    Filemeta meta;
    namespace_get(ns, path, &meta);

    m.server_count = meta.server_count;
    for (int i = 0; i < meta.server_count; i++) {
        strcpy(m.servers[i], meta.servers[i]);
    }

    printf("Sending %d servers for read:", meta.server_count);
    for (int i = 0; i < meta.server_count; i++) {
        printf(" [%s]", m.servers[i]);
    }
    printf("\n");

    write(socket_fd, &m, sizeof(m));
}


void handle_stat(int socket_fd, MSG m){
    char path[PSIZE];
    handle_path(m.path, path);
    printf("Stat for path: %s\n", path);

    Filemeta meta;
    namespace_get(ns, path, &meta);
    m.st = meta.st;
    m.res = 0;

    write(socket_fd, &m, sizeof(m));
}


void handle_update(int socket_fd, MSG m) {
    char path[PSIZE];
    handle_path(m.path, path);
    printf("Update for path: %s\n", path);

    Filemeta meta;
    meta.st = m.st;
    meta.server_count = m.server_count;
    meta.servers = malloc(sizeof(char*) * m.server_count);
    
    for (int i = 0; i < m.server_count; i++) {
        meta.servers[i] = strdup(m.servers[i]);
    }
    
    namespace_add(ns, path, meta);
}


void handle_unlink(int socket_fd, MSG m) {
    char path[PSIZE];
    handle_path(m.path, path);
    printf("Unlink for path: %s\n", path);

    m.res = 0;
    write(socket_fd, &m, sizeof(m));

    Filemeta meta;
    namespace_get(ns, path, &meta);
    
    m.server_count = meta.server_count;
    for (int i = 0; i < meta.server_count; i++) {
        strcpy(m.servers[i], meta.servers[i]);
    }

    int temp = choose_server(&m);
    if (temp < 0) return;
    write(temp, &m, sizeof(m));

    read(temp, &m, sizeof(m));
    if (m.res == -1) {
        perror("Error unlinking file");
    } else {
        printf("File unlinked successfully\n");
        namespace_remove(ns, path);
    }
    close(temp);
}


void handle_mkdir(int socket_fd, MSG m) {
    char path[PSIZE];
    handle_path(m.path, path);
    
    for (int i = 0; i < datanode_count; i++) {
        int replica_fd = connect_server(datanodes[i]);
        if (replica_fd < 0) {
            continue;
        }

        write(replica_fd, &m, sizeof(m));
        close(replica_fd);
    }

    m.res = 0;
    write(socket_fd, &m, sizeof(m));
}    


void handle_rmdir(int socket_fd, MSG m) {
    char path[PSIZE];
    handle_path(m.path, path);

    for (int i = 0; i < datanode_count; i++) {
        int replica_fd = connect_server(datanodes[i]);
        if (replica_fd < 0) {
            continue;
        }

        write(replica_fd, &m, sizeof(m));
        close(replica_fd);
    }

    m.res = 0;
    write(socket_fd, &m, sizeof(m));
}


void* connection_handler(void* arg) {
    int socket_fd = *((int*)arg);
    free(arg);

    MSG m;
    ssize_t res;

    while ((res = read(socket_fd, &m, sizeof(m))) > 0) {
        if (m.op == WRITE) {
            handle_write(socket_fd, m);
        } else if (m.op == UPDATE) {
            handle_update(socket_fd, m);
        } else if (m.op == READ) {
            handle_read(socket_fd, m);
        } else if (m.op == STAT) {
            handle_stat(socket_fd, m);
        } else if (m.op == UNLINK) {
            handle_unlink(socket_fd, m);
        } else if (m.op == MKDIR) {
            handle_mkdir(socket_fd, m);
        } else if (m.op == RMDIR) {
            handle_rmdir(socket_fd, m);
        } else {
            fprintf(stderr, "Unknown operation: %d\n", m.op);
        }
    }

    close(socket_fd);
    return NULL;
}


int main(int argc, char const* argv[])
{
    for (int i = 1; i < argc && i - 1 < MAX_DATANODES; i++) {
        datanodes[datanode_count++] = strdup(argv[i]);
    }
    
    signal(SIGINT, intHandler);
    ns = namespace_init();

    //structure for dealing with internet addresses
    struct sockaddr_in address;

    // Creating socket file descriptor
    // https://man7.org/linux/man-pages/man2/socket.2.html
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    //Initialize struct and parameters
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(5000);

    // Attaches address to socket
    // https://man7.org/linux/man-pages/man2/bind.2.html
    if (bind(server_fd, (struct sockaddr*) &address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen for connections
    //https://man7.org/linux/man-pages/man2/listen.2.html
    if (listen(server_fd, LISTEN_BACKLOG) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    while (1) {
        int new_socket;
        socklen_t addrlen = sizeof(address);
    
        if ((new_socket = accept(server_fd, (struct sockaddr*)&address, &addrlen)) < 0) {
            perror("accept");
            continue;  // continua em vez de sair
        }
    
        int* socket_fd_ptr = malloc(sizeof(int));
        *socket_fd_ptr = new_socket;
    
        pthread_t thread;
        if (pthread_create(&thread, NULL, connection_handler, socket_fd_ptr) != 0) {
            perror("pthread_create");
            close(new_socket);
            free(socket_fd_ptr);
            continue;
        }
    
        pthread_detach(thread);  // não precisas dar join, auto-limpeza
    }
    
    // closing the listening socket
    close(server_fd);
    namespace_destroy(ns);
    return 0;
}