#ifndef NAMESPACE_H
#define NAMESPACE_H

#include <glib.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>

//struct defining the values of the Hashtable 
typedef struct filemeta {
	struct stat st;
	char **servers;
	int server_count;
} Filemeta;

//structure containing the hashtable structure, global mutex and condition variable
typedef struct namespace {
	GHashTable *htable;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
} Namespace;


//Initializes the index structure
//Returns NULL in case of failure and a pointer to the struct otherwise
Namespace* namespace_init();

//Adds a new key-value entry into the Hashtable
//Returns -1 in case of failure (or if the key already exists) and 0 otherwise
int namespace_add(Namespace *space, char* key, Filemeta meta);

//Get the value (meta) for a specific key (key)
//Returns -1 in case of failure (or if the key does not exists) and 0 otherwise
int namespace_get(Namespace *space, char* key, Filemeta *meta);

//Remove a key-value entry from the Hashtable 
//Returns -1 in case of failure (or if the key does not exists) and 0 otherwise
int namespace_remove(Namespace* space, char* key);

//Destroys the index structure
void namespace_destroy(Namespace* space);

#endif // NAMESPACE_H
