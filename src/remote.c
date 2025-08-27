#include "remote.h"
#include "metaindex.h"

static pthread_mutex_t metadata_comm_mutex = PTHREAD_MUTEX_INITIALIZER;

int connect_server(char* address_port) {
    int client_fd;
    struct sockaddr_in serv_addr;
    char ip[INET_ADDRSTRLEN];
    int port;

    if (sscanf(address_port, "%[^:]:%d", ip, &port) != 2) {
        fprintf(stderr, "Formato inválido (esperado ip:port): %s\n", address_port);
        return -1;
    }

    printf("Conectando ao servidor %s na porta %d\n", ip, port);
    
    // Creating socket file descriptor
    // https://man7.org/linux/man-pages/man2/socket.2.html
    if ((client_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return -1;
    }
    
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    
    // Convert IPv4 and IPv6 addresses from text to binary form
    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }
    
    // Connect to server
    if (connect(client_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("\nConnection Failed \n");
        return -1;
    }
    
    return client_fd;
}

void close_server(int client_fd){
    //closing the connected socket
    close(client_fd);
}

int choose_server(MSG *m) {
    static __thread int last_index = 0;
    int attempts = 0;

    while (attempts < m->server_count) {
        int idx = (last_index + attempts) % m->server_count;
        int fd = connect_server(m->servers[idx]);
        if (fd >= 0) {
            last_index = (idx + 1) % m->server_count;
            strncpy(m->primary_server, m->servers[idx], PSIZE - 1);
            m->primary_server[PSIZE - 1] = '\0';
            return fd;
        }
        attempts++;
    }

    return -1;
}

static int safe_msg_communication(int fd, MSG *msg) {
    ssize_t sent = write(fd, msg, sizeof(MSG));
    if (sent != sizeof(MSG)) {
        printf("ERROR: Incomplete write: %zd/%zu bytes\n", sent, sizeof(MSG));
        return -1;
    }
    
    ssize_t received = read(fd, msg, sizeof(MSG));
    if (received != sizeof(MSG)) {
        printf("ERROR: Incomplete read: %zd/%zu bytes\n", received, sizeof(MSG));
        return -1;
    }
    
    return 0;
}

size_t rpwrite(int client_fd, const char* path, const char *buffer, size_t size, off_t offset){
    size_t total_written = 0;

    MSG *m = calloc(1, sizeof(MSG));
    if (!m) {
        printf("ERROR: Failed to allocate MSG for metadata request\n");
        return -1;
    }

    m->op = WRITE;
    strncpy(m->path, path, PSIZE - 1);
    m->path[PSIZE - 1] = '\0';
    m->offset = offset;
    m->size = size;

    pthread_mutex_lock(&metadata_comm_mutex);
    int comm_result = safe_msg_communication(client_fd, m);
    pthread_mutex_unlock(&metadata_comm_mutex);
    
    if (comm_result < 0) {
        free(m);
        return -1;
    }

    int temp = choose_server(m);
    if (temp < 0) {
        free(m);
        return -1;
    }

    int server_count = m->server_count;
    char servers[MAX_REPLICAS][PSIZE];
    char primary_server[PSIZE];
    
    for (int i = 0; i < server_count; i++) {
        strncpy(servers[i], m->servers[i], PSIZE - 1);
        servers[i][PSIZE - 1] = '\0';
    }
    strncpy(primary_server, m->primary_server, PSIZE - 1);
    primary_server[PSIZE - 1] = '\0';
    
    free(m);

    while (total_written < size) {
        MSG *m_chunk = calloc(1, sizeof(MSG));
        if (!m_chunk) {
            printf("ERROR: Failed to allocate MSG for chunk\n");
            close(temp);
            return -1;
        }
        
        m_chunk->op = WRITE;
        strncpy(m_chunk->path, path, PSIZE - 1);
        m_chunk->path[PSIZE - 1] = '\0';

        size_t bytes_to_write = size - total_written;
        if (bytes_to_write > BSIZE) {
            bytes_to_write = BSIZE;
        }

        memcpy(m_chunk->buffer, buffer + total_written, bytes_to_write);
        m_chunk->size = bytes_to_write;
        m_chunk->offset = offset + total_written;
        
        // Restaurar informações dos servidores
        m_chunk->server_count = server_count;
        for (int i = 0; i < server_count; i++) {
            strncpy(m_chunk->servers[i], servers[i], PSIZE - 1);
            m_chunk->servers[i][PSIZE - 1] = '\0';
        }
        strncpy(m_chunk->primary_server, primary_server, PSIZE - 1);
        m_chunk->primary_server[PSIZE - 1] = '\0';

        if (safe_msg_communication(temp, m_chunk) < 0) {
            free(m_chunk);
            close(temp);
            return -1;
        }

        if (m_chunk->res < 0) {
            free(m_chunk);
            close(temp);
            return -1;
        }

        total_written += m_chunk->res;
        free(m_chunk);
    }

    close(temp);
    return total_written;
}

size_t rpread(int client_fd, const char* path, char *buffer, size_t size, off_t offset, Index *cache) {
    size_t total_read = 0;

    MSG *m = calloc(1, sizeof(MSG));
    if (!m) {
        printf("ERROR: Failed to allocate MSG for metadata request\n");
        return -1;
    }

    m->op = READ;
    strncpy(m->path, path, PSIZE - 1);
    m->path[PSIZE - 1] = '\0';
    m->offset = offset;
    m->size = size;

    pthread_mutex_lock(&metadata_comm_mutex);
    int comm_result = safe_msg_communication(client_fd, m);
    pthread_mutex_unlock(&metadata_comm_mutex);
    
    if (comm_result < 0) {
        free(m);
        return -1;
    }

    int temp = choose_server(m);
    free(m);
    
    if (temp < 0) return -1;

    while (total_read < size) {
        MSG *m_chunk = calloc(1, sizeof(MSG));
        if (!m_chunk) {
            printf("ERROR: Failed to allocate MSG for chunk\n");
            close(temp);
            return -1;
        }
        
        m_chunk->op = READ;
        strncpy(m_chunk->path, path, PSIZE - 1);
        m_chunk->path[PSIZE - 1] = '\0';

        size_t bytes_to_read = size - total_read;
        if (bytes_to_read > BSIZE)
            bytes_to_read = BSIZE;

        m_chunk->size = bytes_to_read;
        m_chunk->offset = offset + total_read;

        char cache_key[PSIZE + 64];
        snprintf(cache_key, sizeof(cache_key), "%s:%ld:%ld", m_chunk->path, m_chunk->offset, m_chunk->size);

        Indexmeta meta;
        if (index_get(cache, cache_key, &meta) == 0) {
            size_t copy_size = meta.read_size;
            if (copy_size > (size - total_read)) {
                copy_size = size - total_read;
            }
            memcpy(buffer + total_read, meta.data, copy_size);
            total_read += copy_size;
            free(m_chunk);
            continue;
        }

        if (safe_msg_communication(temp, m_chunk) < 0) {
            free(m_chunk);
            close(temp);
            return total_read;
        }

        if (m_chunk->res <= 0) {
            free(m_chunk);
            break;
        }

        memcpy(buffer + total_read, m_chunk->buffer, m_chunk->res);

        Indexmeta new_meta = {
            .offset = m_chunk->offset,
            .size = m_chunk->size,
            .read_size = m_chunk->res,
            .data = malloc(m_chunk->res)
        };
        memcpy(new_meta.data, m_chunk->buffer, m_chunk->res);
        index_add(cache, cache_key, new_meta);

        total_read += m_chunk->res;
        free(m_chunk);
    }

    close(temp);
    return total_read;
}

int rstat(int client_fd, const char* path, struct stat *stbuf){
    MSG *m = calloc(1, sizeof(MSG));
    if (!m) {
        printf("ERROR: Failed to allocate MSG for stat request\n");
        return -1;
    }
    
    m->op = STAT;
    strncpy(m->path, path, PSIZE - 1);
    m->path[PSIZE - 1] = '\0';

    pthread_mutex_lock(&metadata_comm_mutex);
    int comm_result = safe_msg_communication(client_fd, m);
    pthread_mutex_unlock(&metadata_comm_mutex);
    
    if (comm_result < 0) {
        free(m);
        return -1;
    }

    printf("Client: Stat request sent for path: %s\n", path);

    int result = m->res;
    if (result == 0) {
        memcpy(stbuf, &m->st, sizeof(struct stat));
        printf("Client: Received stat response, size: %ld\n", m->st.st_size);
    } else {
        printf("Client: Stat failed with error: %d\n", result);
    }
    
    free(m);
    return result;
}

int rpunlink(int client_fd, const char* path) {
    MSG *m = calloc(1, sizeof(MSG));
    if (!m) {
        printf("ERROR: Failed to allocate MSG for unlink request\n");
        return -1;
    }
    
    m->op = UNLINK;
    strncpy(m->path, path, PSIZE - 1);
    m->path[PSIZE - 1] = '\0';

    pthread_mutex_lock(&metadata_comm_mutex);
    int comm_result = safe_msg_communication(client_fd, m);
    pthread_mutex_unlock(&metadata_comm_mutex);
    
    if (comm_result < 0) {
        free(m);
        return -1;
    }

    printf("Client: Unlink request sent for path: %s\n", m->path);
    printf("Client: Unlink response received, result: %zd\n", m->res);
    
    int result = m->res;
    free(m);
    return result;
}

int rpmkdir(int client_fd, const char* path, mode_t mode) {
    MSG *m = calloc(1, sizeof(MSG));
    if (!m) {
        printf("ERROR: Failed to allocate MSG for mkdir request\n");
        return -1;
    }
    
    m->op = MKDIR;
    strncpy(m->path, path, PSIZE - 1);
    m->path[PSIZE - 1] = '\0';
    m->mode = mode;

    pthread_mutex_lock(&metadata_comm_mutex);
    int comm_result = safe_msg_communication(client_fd, m);
    pthread_mutex_unlock(&metadata_comm_mutex);
    
    if (comm_result < 0) {
        free(m);
        return -1;
    }

    printf("Client: Mkdir request sent for path: %s\n", path);
    printf("Client: Mkdir response received, result: %zd\n", m->res);

    int result = m->res;
    free(m);
    return result;
}

int rprmdir(int client_fd, const char* path) {
    MSG *m = calloc(1, sizeof(MSG));
    if (!m) {
        printf("ERROR: Failed to allocate MSG for rmdir request\n");
        return -1;
    }
    
    m->op = RMDIR;
    strncpy(m->path, path, PSIZE - 1);
    m->path[PSIZE - 1] = '\0';

    pthread_mutex_lock(&metadata_comm_mutex);
    int comm_result = safe_msg_communication(client_fd, m);
    pthread_mutex_unlock(&metadata_comm_mutex);
    
    if (comm_result < 0) {
        free(m);
        return -1;
    }

    printf("Client: Rmdir request sent for path: %s\n", path);
    printf("Client: Rmdir response received, result: %zd\n", m->res);
    
    int result = m->res;
    free(m);
    return result;
}