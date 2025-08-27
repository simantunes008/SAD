#include "metaindex.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void free_indexmeta(gpointer data) {
    Indexmeta *meta = (Indexmeta*)data;
    if (meta) {
        if (meta->data) {
            free(meta->data);
        }
        free(meta);
    }
}

void* eviction_thread_func(void *arg) {
    EvictionThreadData *thread_data = (EvictionThreadData*)arg;
    Index *index = thread_data->index;
    
    while (!thread_data->should_stop) {
        pthread_mutex_lock(&index->mutex);
        
        while (g_queue_get_length(index->lru_queue) <= MAX_CACHE_ENTRIES && !thread_data->should_stop) { 
            pthread_cond_wait(&index->cond, &index->mutex);
        }
        
        if (thread_data->should_stop) {
            pthread_mutex_unlock(&index->mutex);
            break;
        }
        
        if (g_queue_get_length(index->lru_queue) > MAX_CACHE_ENTRIES) {
            char *lru_key = g_queue_pop_head(index->lru_queue);
            if (lru_key != NULL) {
                g_hash_table_remove(index->htable, lru_key);
            }
        }
        
        pthread_mutex_unlock(&index->mutex);
    }
    
    return NULL;
}

Index* index_init() {
    Index *index = malloc(sizeof(Index));
    if (!index) {
        return NULL;
    }
    
    index->htable = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, free_indexmeta);
    if (!index->htable) {
        free(index);
        return NULL;
    }
    
    index->lru_queue = g_queue_new();
    if (!index->lru_queue) {
        g_hash_table_destroy(index->htable);
        free(index);
        return NULL;
    }
    
    if (pthread_mutex_init(&index->mutex, NULL) != 0) {
        g_queue_free(index->lru_queue);
        g_hash_table_destroy(index->htable);
        free(index);
        return NULL;
    }
    
    if (pthread_cond_init(&index->cond, NULL) != 0) {
        pthread_mutex_destroy(&index->mutex);
        g_queue_free(index->lru_queue);
        g_hash_table_destroy(index->htable);
        free(index);
        return NULL;
    }
    
    index->thread_data = malloc(sizeof(EvictionThreadData));
    if (!index->thread_data) {
        pthread_cond_destroy(&index->cond);
        pthread_mutex_destroy(&index->mutex);
        g_queue_free(index->lru_queue);
        g_hash_table_destroy(index->htable);
        free(index);
        return NULL;
    }
    
    index->thread_data->index = index;
    index->thread_data->should_stop = 0;
    
    if (pthread_create(&index->eviction_thread, NULL, eviction_thread_func, (void*)index->thread_data) != 0) {
        free(index->thread_data);
        pthread_cond_destroy(&index->cond);
        pthread_mutex_destroy(&index->mutex);
        g_queue_free(index->lru_queue);
        g_hash_table_destroy(index->htable);
        free(index);
        return NULL;
    }
    
    return index;
}

static void lru_update(Index *index, const char *key) {
    GList *node = g_queue_find_custom(index->lru_queue, key, (GCompareFunc)strcmp);
    if (node) {
        g_queue_unlink(index->lru_queue, node);
        g_queue_push_tail_link(index->lru_queue, node);
    }
}

int index_add(Index *index, char* key, Indexmeta meta) {
    if (!index || !key) {
        return -1;
    }
    
    pthread_mutex_lock(&index->mutex);
    
    if (g_hash_table_contains(index->htable, key)) {
        pthread_mutex_unlock(&index->mutex);
        return -1;
    }
    
    char *nkey = strdup(key);
    if (!nkey) {
        pthread_mutex_unlock(&index->mutex);
        return -1;
    }
    
    Indexmeta *value = malloc(sizeof(Indexmeta));
    if (!value) {
        free(nkey);
        pthread_mutex_unlock(&index->mutex);
        return -1;
    }
    
    value->offset = meta.offset;
    value->size = meta.size;
    value->read_size = meta.read_size;
    value->data = NULL;
    
    if (meta.data && meta.read_size > 0) {
        value->data = malloc(meta.read_size);
        if (!value->data) {
            free(value);
            free(nkey);
            pthread_mutex_unlock(&index->mutex);
            return -1;
        }
        memcpy(value->data, meta.data, meta.read_size);
    }
    
    g_hash_table_insert(index->htable, nkey, value);
    g_queue_push_tail(index->lru_queue, nkey);
    
    if (g_queue_get_length(index->lru_queue) > MAX_CACHE_ENTRIES) {
        pthread_cond_signal(&index->cond);
    }
    
    pthread_mutex_unlock(&index->mutex);
    return 0;
}

int index_get(Index *index, char* key, Indexmeta *meta) {
    if (!index || !key || !meta) {
        return -1;
    }
    
    pthread_mutex_lock(&index->mutex);
    
    Indexmeta *res = g_hash_table_lookup(index->htable, key);
    if (!res) {
        pthread_mutex_unlock(&index->mutex);
        return -1;
    }

    meta->offset = res->offset;
    meta->size = res->size;
    meta->read_size = res->read_size;
    meta->data = NULL;
    
    if (res->data && res->read_size > 0) {
        meta->data = malloc(res->read_size);
        if (!meta->data) {
            pthread_mutex_unlock(&index->mutex);
            return -1;
        }
        memcpy(meta->data, res->data, res->read_size);
    }
    
    lru_update(index, key);
    
    pthread_mutex_unlock(&index->mutex);
    return 0;
}

int index_remove(Index* index, char* key) {
    if (!index || !key) {
        return -1;
    }
    
    pthread_mutex_lock(&index->mutex);
    
    if (!g_hash_table_contains(index->htable, key)) {
        pthread_mutex_unlock(&index->mutex);
        return -1;
    }
    
    GList *node = g_queue_find_custom(index->lru_queue, key, (GCompareFunc)strcmp);
    if (node) {
        g_queue_delete_link(index->lru_queue, node);
    }
    
    gboolean removed = g_hash_table_remove(index->htable, key);
    
    pthread_mutex_unlock(&index->mutex);
    return removed ? 0 : -1;
}

void index_destroy(Index* index) {
    if (!index) {
        return;
    }
    
    pthread_mutex_lock(&index->mutex);
    if (index->thread_data) {
        index->thread_data->should_stop = 1;
        pthread_cond_signal(&index->cond);
    }
    pthread_mutex_unlock(&index->mutex);
    
    pthread_join(index->eviction_thread, NULL);
    
    if (index->thread_data) {
        free(index->thread_data);
    }
    
    if (index->htable) {
        g_hash_table_destroy(index->htable);
    }
    
    if (index->lru_queue) {
        g_queue_free(index->lru_queue);
    }
    
    pthread_mutex_destroy(&index->mutex);
    pthread_cond_destroy(&index->cond);
    
    free(index);
}