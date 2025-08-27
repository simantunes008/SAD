#ifndef METAINDEX_H
#define METAINDEX_H

#include <glib.h>
#include <pthread.h>
#include <stddef.h>
#include <sys/types.h>

typedef struct indexmeta {
    size_t offset;
    size_t size;
    ssize_t read_size;
    char *data;
} Indexmeta;

typedef struct eviction_thread_data {
    struct index *index;
    int should_stop;
} EvictionThreadData;

typedef struct index {
    GHashTable *htable;
    GQueue *lru_queue;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    pthread_t eviction_thread;
    EvictionThreadData *thread_data;
} Index;

#define MAX_CACHE_ENTRIES 25000

Index* index_init();

int index_add(Index *index, char* key, Indexmeta meta);

int index_get(Index *index, char* key, Indexmeta *meta);

int index_remove(Index* index, char* key);

void index_destroy(Index* index);

#endif // METAINDEX_H
