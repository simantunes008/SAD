#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/stat.h>

#include "metaindex.h"

#define LISTEN_BACKLOG 50
#define BSIZE 4096
#define PSIZE 512
#define READ 0
#define WRITE 1
#define STAT 2
#define UNLINK 3
#define UPDATE 4
#define REPLICA 5
#define UNLINK_REPLICA 6
#define MKDIR 7
#define RMDIR 8

#define MAX_REPLICAS 3

typedef struct msg{
	int op;
	char path[PSIZE];
	char buffer[BSIZE];
	char servers[MAX_REPLICAS][PSIZE];
    int server_count;
	char primary_server[PSIZE];
	off_t offset;
	size_t size;
	ssize_t res;
	mode_t mode;
	struct stat st;
} MSG;


int connect_server(char* address_port);

void close_server(int client_fd);

int choose_server(MSG *m);

size_t rpwrite(int client_fd, const char* path, const char *buffer, size_t size, off_t offset);

size_t rpread(int client_fd, const char* path, char *buffer, size_t size, off_t offset, Index *cache);

int rstat(int client_fd, const char* path, struct stat *stbuf);

int rpunlink(int client_fd, const char* path);

int rpmkdir(int client_fd, const char* path, mode_t mode);

int rprmdir(int client_fd, const char* path);
