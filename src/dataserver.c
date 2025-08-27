#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <fcntl.h>
#include "remote.h"
#include "metaindex.h"
#include "serverutils.h"

int server_fd;
int client_fd;

void intHandler(int dummy) {
    printf("DataNode shutting down\n");
    close(server_fd);
    exit(0);   
}


void handle_write(int socket_fd, MSG m){
    char path[PSIZE];
    handle_path(m.path, path);
    printf("DataNode: handling write for path: %s\n", path);
    
    int fd = open(path, O_WRONLY | O_CREAT, 0644);
    if (fd == -1) {
        perror("DataNode error opening file");
        m.res = -errno;
    } else {
        m.res = pwrite(fd, m.buffer, m.size, m.offset);
        if (m.res == -1) {
            m.res = -errno;
            perror("DataNode error writing to file");
        }
        close(fd);
    }

    printf("DataNode: write result: %zd for path: %s\n", m.res, path);
    write(socket_fd, &m, sizeof(m));

    stat(path, &m.st);
    if (m.res == -1) {
        m.res = -errno;
        perror("DataNode error getting file stats");
    }

    m.op = UPDATE;
    write(client_fd, &m, sizeof(m));
}


void handle_read(int socket_fd, MSG m) {
    char path[PSIZE];
    handle_path(m.path, path);
    printf("DataNode: handling read for path: %s\n", path);

    char cache_key[PSIZE + 64];
    snprintf(cache_key, sizeof(cache_key), "%s:%ld:%ld", m.path, m.offset, m.size);

    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        perror("DataNode error opening file for reading");
        m.res = -errno;
    } else {
        m.res = pread(fd, m.buffer, m.size, m.offset);
        if (m.res == -1) {
            m.res = -errno;
            perror("DataNode error reading from file");
        }
        close(fd);
    }

    printf("DataNode: read result: %zd for path: %s\n", m.res, path);
    write(socket_fd, &m, sizeof(m));
}


void handle_unlink(int socket_fd, MSG m) {
    char path[PSIZE];
    handle_path(m.path, path);
    printf("DataNode: handling unlink for path: %s\n", path);

    m.res = unlink(path);
    if (m.res == -1) {
        m.res = -errno;
        perror("DataNode error unlinking file");
    }
    
    printf("DataNode: unlink result: %zd for path: %s\n", m.res, path);
    write(socket_fd, &m, sizeof(m));
}


void handle_replica(int socket_fd, MSG m){
    char path[PSIZE];
    handle_path(m.path, path);
    printf("DataNode: handling replica for path: %s\n", path);
    
    int fd = open(path, O_WRONLY | O_CREAT, 0644);
    if (fd == -1) {
        perror("DataNode error opening file");
        m.res = -errno;
    } else {
        m.res = pwrite(fd, m.buffer, m.size, m.offset);
        if (m.res == -1) {
            m.res = -errno;
            perror("DataNode error writing to file");
        }
        close(fd);
    }
}


void handle_mkdir(int socket_fd, MSG m) {
    char path[PSIZE];
    handle_path(m.path, path);
    printf("Server: handling mkdir for path: %s\n", path);

    m.res = mkdir(path, m.mode);
    if (m.res == -1) {
        m.res = -errno;
    }
    
    write(socket_fd, &m, sizeof(m));
}


void handle_rmdir(int socket_fd, MSG m) {
    char path[PSIZE];
    handle_path(m.path, path);
    printf("Server: handling rmdir for path: %s\n", path);

    m.res = rmdir(path);
    if (m.res == -1) {
        m.res = -errno;
    }
    
    write(socket_fd, &m, sizeof(m));
}


void* replication_thread(void* arg) {
    MSG* m = (MSG*)arg;
    
    for (int i = 0; i < m->server_count; i++) {
        if (strcmp(m->servers[i], m->primary_server) == 0) {
            continue;
        }
        
        int replica_fd = connect_server(m->servers[i]);
        if (replica_fd < 0) {
            continue;
        }
        
        MSG replica_msg = *m;
        replica_msg.op = REPLICA;
        
        write(replica_fd, &replica_msg, sizeof(replica_msg));
        close(replica_fd);
    }
    
    free(m);
    return NULL;
}

void* deletion_thread(void* arg) {
    MSG* m = (MSG*)arg;
    
    for (int i = 0; i < m->server_count; i++) {
        if (strcmp(m->servers[i], m->primary_server) == 0) {
            continue;
        }
        
        int replica_fd = connect_server(m->servers[i]);
        if (replica_fd < 0) {
            continue;
        }
        
        MSG replica_msg = *m;
        replica_msg.op = UNLINK_REPLICA;

        write(replica_fd, &replica_msg, sizeof(replica_msg));
        close(replica_fd);
    }
    
    free(m);
    return NULL;
}


void* connection_handler(void* arg) {
    int socket_fd = *((int*)arg);
    free(arg);

    MSG m;
    ssize_t res;

    while ((res = read(socket_fd, &m, sizeof(m))) > 0) {
        if (m.op == WRITE) {
            handle_write(socket_fd, m);
            MSG* m_copy = malloc(sizeof(MSG));
            *m_copy = m;

            pthread_t rep_thread;
            if (pthread_create(&rep_thread, NULL, replication_thread, m_copy) != 0) {
                perror("Erro ao criar thread de replicação");
                free(m_copy);
            } else {
                pthread_detach(rep_thread);
            }
        } else if (m.op == READ) {
            handle_read(socket_fd, m);
        } else if (m.op == UNLINK) {
            handle_unlink(socket_fd, m);
            MSG* m_copy = malloc(sizeof(MSG));
            *m_copy = m;

            pthread_t del_thread;
            if (pthread_create(&del_thread, NULL, deletion_thread, m_copy) != 0) {
                perror("Erro ao criar thread de replicação");
                free(m_copy);
            } else {
                pthread_detach(del_thread);
            }
        } else if (m.op == REPLICA) {
            handle_replica(socket_fd, m);
        } else if (m.op == UNLINK_REPLICA) {
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
    client_fd = connect_server((char*)argv[1]);

    signal(SIGINT, intHandler);

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
    address.sin_port = htons(5001);

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
    close(client_fd);
    return 0;
}