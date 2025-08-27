#include "namespace.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_REPLICAS 3
#define LOG_PATH "/home/vagrant/server/fs.log"
static FILE *log_file = NULL;

static void filemeta_free(gpointer data) {
    Filemeta *meta = (Filemeta*)data;
    if (meta) {
        for (int i = 0; i < meta->server_count; i++) {
            free(meta->servers[i]);
        }
        free(meta->servers);
        free(meta);
    }
}

// Função auxiliar para alocar e copiar Filemeta
static Filemeta* filemeta_copy(const Filemeta *src) {
    if (!src) return NULL;
    
    Filemeta *dst = malloc(sizeof(Filemeta));
    if (!dst) return NULL;
    
    dst->st = src->st;
    dst->server_count = src->server_count;
    
    if (src->server_count > 0) {
        dst->servers = malloc(sizeof(char*) * src->server_count);
        if (!dst->servers) {
            free(dst);
            return NULL;
        }
        
        for (int i = 0; i < src->server_count; i++) {
            dst->servers[i] = strdup(src->servers[i]);
            if (!dst->servers[i]) {
                // Limpa alocações parciais em caso de erro
                for (int j = 0; j < i; j++) {
                    free(dst->servers[j]);
                }
                free(dst->servers);
                free(dst);
                return NULL;
            }
        }
    } else {
        dst->servers = NULL;
    }
    
    return dst;
}


Namespace* namespace_init(){
    // Allocate memory for the space
    Namespace *space = malloc(sizeof(Namespace));
    if (!space) {
        return NULL;
    }

    // GHashTable initialization com função de limpeza correta
    space->htable = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, filemeta_free);
    if (!space->htable) {
        free(space);
        return NULL;
    }

    // Mutex variable initialization
    if (pthread_mutex_init(&space->mutex, NULL) != 0) {
        g_hash_table_destroy(space->htable);
        free(space);
        return NULL;
    }
    
    // Condition variable initialization
    if (pthread_cond_init(&space->cond, NULL) != 0) {
        pthread_mutex_destroy(&space->mutex);
        g_hash_table_destroy(space->htable);
        free(space);
        return NULL;
    }

    // Open the log file
    log_file = fopen(LOG_PATH, "a+");
    if (!log_file) {
        perror("fopen log");
        pthread_cond_destroy(&space->cond);
        pthread_mutex_destroy(&space->mutex);
        g_hash_table_destroy(space->htable);
        free(space);
        return NULL;
    }

    char op[16], path[512], servers_str[1024];
    time_t mtime;
    off_t size;

    // Read the log file and populate the hashtable
    rewind(log_file);
    while (fscanf(log_file, "%s", op) == 1) {
        if (strcmp(op, "ADD") == 0) {
            if (fscanf(log_file, "%s %s %ld %ld", path, servers_str, &mtime, &size) != 4) {
                continue; // Skip malformed entries
            }
              
            Filemeta meta;
            meta.st.st_mtime = mtime;
            meta.st.st_size = size;
            meta.server_count = 0;
            meta.servers = malloc(sizeof(char*) * MAX_REPLICAS);
            if (!meta.servers) {
                continue; // Skip this entry on allocation failure
            }
            
            char *token = strtok(servers_str, ",");
            while (token != NULL && meta.server_count < MAX_REPLICAS) {
                meta.servers[meta.server_count] = strdup(token);
                if (!meta.servers[meta.server_count]) {
                    // Cleanup on strdup failure
                    for (int i = 0; i < meta.server_count; i++) {
                        free(meta.servers[i]);
                    }
                    free(meta.servers);
                    goto skip_entry;
                }
                meta.server_count++;
                token = strtok(NULL, ",");
            }
            
            pthread_mutex_lock(&space->mutex);
            
            char *nkey = strdup(path);
            if (!nkey) {
                pthread_mutex_unlock(&space->mutex);
                // Cleanup meta
                for (int i = 0; i < meta.server_count; i++) {
                    free(meta.servers[i]);
                }
                free(meta.servers);
                continue;
            }
            
            Filemeta *value = filemeta_copy(&meta);
            if (!value) {
                pthread_mutex_unlock(&space->mutex);
                free(nkey);
                // Cleanup meta
                for (int i = 0; i < meta.server_count; i++) {
                    free(meta.servers[i]);
                }
                free(meta.servers);
                continue;
            }
            
            g_hash_table_insert(space->htable, nkey, value);
            pthread_mutex_unlock(&space->mutex);
            
            // Cleanup temporary meta (CORREÇÃO DO MEMORY LEAK)
            for (int i = 0; i < meta.server_count; i++) {
                free(meta.servers[i]);
            }
            free(meta.servers);
            
        } else if (strcmp(op, "REMOVE") == 0) {
            if (fscanf(log_file, "%s", path) != 1) {
                continue; // Skip malformed entries
            }
            pthread_mutex_lock(&space->mutex);
            g_hash_table_remove(space->htable, path);
            pthread_mutex_unlock(&space->mutex);
        }
        
        skip_entry:
        continue;
    }

    return space;
}

int namespace_add(Namespace *space, char* key, Filemeta meta){
    if (!space || !key) {
        return -1;
    }

    pthread_mutex_lock(&space->mutex);

    char *nkey = strdup(key);
    if (!nkey) {
        pthread_mutex_unlock(&space->mutex);
        return -1;
    }
    
    Filemeta *value = filemeta_copy(&meta);
    if (!value) {
        free(nkey);
        pthread_mutex_unlock(&space->mutex);
        return -1;
    }

    g_hash_table_insert(space->htable, nkey, value);

    // Write to log
    fprintf(log_file, "ADD %s ", key);
    for (int i = 0; i < value->server_count; i++) {
        fprintf(log_file, "%s", value->servers[i]);
        if (i < value->server_count - 1) fprintf(log_file, ",");
    }
    fprintf(log_file, " %ld %ld\n", value->st.st_mtime, value->st.st_size);
    fflush(log_file);

    pthread_mutex_unlock(&space->mutex);
    return 0;
}

int namespace_get(Namespace *space, char* key, Filemeta *meta){
    if (!space || !key || !meta) {
        return -1;
    }

    pthread_mutex_lock(&space->mutex);
    Filemeta *res = g_hash_table_lookup(space->htable, key);
    if(res == NULL){
        pthread_mutex_unlock(&space->mutex);
        return -1;
    }
    
    meta->st = res->st;
    meta->server_count = res->server_count;

    if (res->server_count > 0) {
        meta->servers = malloc(sizeof(char*) * res->server_count);
        if (!meta->servers) {
            pthread_mutex_unlock(&space->mutex);
            return -1;
        }
        
        for (int i = 0; i < res->server_count; i++) {
            meta->servers[i] = strdup(res->servers[i]);
            if (!meta->servers[i]) {
                // Cleanup on failure
                for (int j = 0; j < i; j++) {
                    free(meta->servers[j]);
                }
                free(meta->servers);
                pthread_mutex_unlock(&space->mutex);
                return -1;
            }
        }
    } else {
        meta->servers = NULL;
    }

    pthread_mutex_unlock(&space->mutex);
    return 0;
}

int namespace_remove(Namespace* space, char* key){
    if (!space || !key) {
        return -1;
    }

    pthread_mutex_lock(&space->mutex);
    
    // Check if key exists before attempting removal
    if (!g_hash_table_lookup(space->htable, key)) {
        pthread_mutex_unlock(&space->mutex);
        return -1;
    }

    // Remove from hash table (filemeta_free será chamada automaticamente)
    gboolean removed = g_hash_table_remove(space->htable, key);
    
    if (removed) {
        fprintf(log_file, "REMOVE %s\n", key);
        fflush(log_file);
    }
    
    pthread_mutex_unlock(&space->mutex);
    return removed ? 0 : -1;
}

void namespace_destroy(Namespace* space){
    if (!space) {
        return;
    }

    // Destroy hashtable (filemeta_free será chamada para cada valor)
    if (space->htable) {
        g_hash_table_destroy(space->htable);
    }

    // Destroy mutex and cond variables
    pthread_mutex_destroy(&space->mutex);
    pthread_cond_destroy(&space->cond);

    if (log_file) {
        fclose(log_file);
        log_file = NULL;
    }

    free(space);
}