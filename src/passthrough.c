/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2011       Sebastian Pipping <sebastian@pipping.org>

  This program can be distributed under the terms of the GNU GPLv2.
  See the file COPYING.
*/

/** @file
 *
 * This file system mirrors the existing file system hierarchy of the
 * system, starting at the root file system. This is implemented by
 * just "passing through" all requests to the corresponding user-space
 * libc functions. Its performance is terrible.
 *
 * Compile with
 *
 *     gcc -Wall passthrough.c `pkg-config fuse3 --cflags --libs` -o passthrough
 *
 * ## Source code ##
 * \include passthrough.c
 */


#define FUSE_USE_VERSION 31

#define _GNU_SOURCE

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <stdlib.h>
#include <pthread.h>
#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#ifdef __FreeBSD__
#include <sys/socket.h>
#include <sys/un.h>
#endif
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include "passthrough_helpers.h"
#include "metaindex.h"

#define DEBUG "/tmp/debug.log"

#ifdef DEBUG
typedef struct context{
	uint64_t open;
	uint64_t close;
	uint64_t write;
	uint64_t read;
	pthread_mutex_t mutex;
	FILE* fp;
	int client_fd;
	Index* cache;
} Context;
#endif

static int fill_dir_plus = 0;

static char server_address[64] = "127.0.0.1:5000";

static void *xmp_init(struct fuse_conn_info *conn,
		      struct fuse_config *cfg)
{
	(void) conn;
	cfg->use_ino = 1;

	/* Pick up changes from lower filesystem right away. This is
	   also necessary for better hardlink support. When the kernel
	   calls the unlink() handler, it does not know the inode of
	   the to-be-removed entry and can therefore not invalidate
	   the cache of the associated inode - resulting in an
	   incorrect st_nlink value being reported for any remaining
	   hardlinks to this inode. */
	cfg->entry_timeout = 0;
	cfg->attr_timeout = 0;
	cfg->negative_timeout = 0;

	//Initialize context
	Context *ctx=malloc(sizeof(Context));
	struct fuse_context *f_ctx = fuse_get_context();
	pthread_mutex_init(&ctx->mutex, NULL);

	//Connect to server
	ctx->client_fd = connect_server(server_address);
	//Initialize cache
	ctx->cache = index_init();

#ifdef DEBUG
	ctx->open=0;
	ctx->close=0;
	ctx->read=0;
	ctx->write=0;	
	ctx->fp = fopen(DEBUG, "w");
	printf("[Thread %d] Init called, userid %d, pid %d\n", gettid(), f_ctx->uid, f_ctx->pid);
	fprintf(ctx->fp,"[Thread %d] Init called, userid %d, pid %d\n", gettid(), f_ctx->uid, f_ctx->pid);
#endif

	return ctx;

}

static void xmp_destroy(void* private_data){


	struct fuse_context *f_ctx = fuse_get_context();
	Context* p_ctx = (Context*) private_data;
	pthread_mutex_destroy(&p_ctx->mutex);

	//Disconnect from server
	close_server(p_ctx->client_fd);
	//Free cache
	index_destroy(p_ctx->cache);

#ifdef DEBUG
	printf("[Thread %d] Destroy called, userid %d, pid %d\n", gettid(), f_ctx->uid, f_ctx->pid);
	printf("[Thread %d] Open() - %lu, Read() - %lu, Write() - %lu, Close() - %lu\n", gettid(), p_ctx->open,p_ctx->read,p_ctx->write,p_ctx->close);
  fprintf(p_ctx->fp,"[Thread %d] Destroy called, userid %d, pid %d\n", gettid(), f_ctx->uid, f_ctx->pid);
	fprintf(p_ctx->fp,"[Thread %d] Open() - %lu, Read() - %lu, Write() - %lu, Close() - %lu\n", gettid(), p_ctx->open,p_ctx->read,p_ctx->write,p_ctx->close);
	fclose(p_ctx->fp);
#endif

	free(private_data);

}

static int xmp_getattr(const char *path, struct stat *stbuf,
		       struct fuse_file_info *fi)
{
	(void) fi;
	int res;

	res = lstat(path, stbuf);

	//below we have already the modified code for ex3
	//TODO implement the rstat function!
	struct fuse_context *f_ctx = fuse_get_context();
	Context *p_ctx = f_ctx->private_data;
	pthread_mutex_lock(&p_ctx->mutex);

	struct stat staux;
	int remote_res = rstat(p_ctx->client_fd, path, &staux);
	if(remote_res==0){
		stbuf->st_size = staux.st_size;
	}

	pthread_mutex_unlock(&p_ctx->mutex);

	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_access(const char *path, int mask)
{
	int res;

	res = access(path, mask);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_readlink(const char *path, char *buf, size_t size)
{
	int res;


	res = readlink(path, buf, size - 1);
	if (res == -1)
		return -errno;

	buf[res] = '\0';
	return 0;
}


static int xmp_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi,
		       enum fuse_readdir_flags flags)
{
	DIR *dp;
	struct dirent *de;

	(void) offset;
	(void) fi;
	(void) flags;

	dp = opendir(path);
	if (dp == NULL)
		return -errno;

	while ((de = readdir(dp)) != NULL) {
		struct stat st;
		memset(&st, 0, sizeof(st));
		st.st_ino = de->d_ino;
		st.st_mode = de->d_type << 12;
		if (filler(buf, de->d_name, &st, 0, fill_dir_plus))
			break;
	}

	closedir(dp);
	return 0;
}

static int xmp_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res;

	res = mknod_wrapper(AT_FDCWD, path, NULL, mode, rdev);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_unlink(const char *path)
{
	struct fuse_context *f_ctx = fuse_get_context();
    Context *p_ctx = f_ctx->private_data;
    int res;
    
    pthread_mutex_lock(&p_ctx->mutex);
    
    res = rpunlink(p_ctx->client_fd, path);
    
    #ifdef DEBUG
    printf("[Thread %d] Unlink for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
    fprintf(p_ctx->fp,"[Thread %d] Unlink for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
    #endif
    
    pthread_mutex_unlock(&p_ctx->mutex);
    
	if (res == 0) {
		unlink(path);
	}
    
    return res;
}

static int xmp_mkdir(const char *path, mode_t mode)
{
	struct fuse_context *f_ctx = fuse_get_context();
    Context *p_ctx = f_ctx->private_data;
	int res;

	res = rpmkdir(p_ctx->client_fd, path, mode);;

	res = mkdir(path, mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_rmdir(const char *path)
{
	struct fuse_context *f_ctx = fuse_get_context();
    Context *p_ctx = f_ctx->private_data;
	int res;

	res = rprmdir(p_ctx->client_fd, path);
	
	res = rmdir(path);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_symlink(const char *from, const char *to)
{
	int res;

	res = symlink(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_rename(const char *from, const char *to, unsigned int flags)
{
	int res;

	if (flags)
		return -EINVAL;

	res = rename(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_link(const char *from, const char *to)
{
	int res;

	res = link(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_chmod(const char *path, mode_t mode,
		     struct fuse_file_info *fi)
{
	(void) fi;
	int res;

	res = chmod(path, mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_chown(const char *path, uid_t uid, gid_t gid,
		     struct fuse_file_info *fi)
{
	(void) fi;
	int res;

	res = lchown(path, uid, gid);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_truncate(const char *path, off_t size,
			struct fuse_file_info *fi)
{
	int res;

	if (fi != NULL)
		res = ftruncate(fi->fh, size);
	else
		res = truncate(path, size);
	if (res == -1)
		return -errno;

	return 0;
}

#ifdef HAVE_UTIMENSAT
static int xmp_utimens(const char *path, const struct timespec ts[2],
		       struct fuse_file_info *fi)
{
	(void) fi;
	int res;

	/* don't use utime/utimes since they follow symlinks */
	res = utimensat(0, path, ts, AT_SYMLINK_NOFOLLOW);
	if (res == -1)
		return -errno;

	return 0;
}
#endif

static int xmp_create(const char *path, mode_t mode,
		      struct fuse_file_info *fi)
{
	int res;

#ifdef DEBUG
	struct fuse_context *f_ctx = fuse_get_context();
	Context *p_ctx = f_ctx->private_data;
	pthread_mutex_lock(&p_ctx->mutex);
	p_ctx->open++;
	printf("[Thread %d] Create for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
	fprintf(p_ctx->fp,"[Thread %d] Create for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
	pthread_mutex_unlock(&p_ctx->mutex);
#endif

	res = open(path, fi->flags, mode);
	if (res == -1)
		return -errno;

	fi->fh = res;
	return 0;
}

static int xmp_open(const char *path, struct fuse_file_info *fi)
{
	int res;

#ifdef DEBUG
	struct fuse_context *f_ctx = fuse_get_context();
	Context *p_ctx = (Context *) f_ctx->private_data;
	pthread_mutex_lock(&p_ctx->mutex);
	p_ctx->open++;
	printf("[Thread %d] Open for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
	fprintf(p_ctx->fp,"[Thread %d] Open for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
	pthread_mutex_unlock(&p_ctx->mutex);
#endif

	res = open(path, fi->flags);
	if (res == -1)
		return -errno;

	fi->fh = res;
	return 0;
}


static int xmp_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	int res;

	struct fuse_context *f_ctx = fuse_get_context();
	Context *p_ctx = f_ctx->private_data;

#ifdef DEBUG
	pthread_mutex_lock(&p_ctx->mutex);
	p_ctx->read++;
	printf("[Thread %d] Read for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
	fprintf(p_ctx->fp,"[Thread %d] Read for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
	pthread_mutex_unlock(&p_ctx->mutex);
#endif

	res = rpread(p_ctx->client_fd, path, buf, size, offset, p_ctx->cache);

	return res;
}

static int xmp_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	int res;

	struct fuse_context *f_ctx = fuse_get_context();
	Context *p_ctx = f_ctx->private_data;

#ifdef DEBUG
	pthread_mutex_lock(&p_ctx->mutex);
	p_ctx->write++;
	printf("[Thread %d] Write for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
	fprintf(p_ctx->fp,"[Thread %d] Write for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
	pthread_mutex_unlock(&p_ctx->mutex);
#endif

	res = rpwrite(p_ctx->client_fd, path, buf, size, offset);

	return res;
}

static int xmp_statfs(const char *path, struct statvfs *stbuf)
{
	int res;
	res = statvfs(path, stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_release(const char *path, struct fuse_file_info *fi)
{
	(void) path;


#ifdef DEBUG
	struct fuse_context *f_ctx = fuse_get_context();
	Context *p_ctx = f_ctx->private_data;
	pthread_mutex_lock(&p_ctx->mutex);
	p_ctx->close++;
	printf("[Thread %d] Close for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
	fprintf(p_ctx->fp,"[Thread %d] Close for path %s, userid %d, pid %d\n", gettid(), path, f_ctx->uid, f_ctx->pid);
	pthread_mutex_unlock(&p_ctx->mutex);
#endif

	
	return close(fi->fh);
}

static int xmp_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{

	(void) path;

	//If the datasync parameter is non-zero, then only the user data should be flushed, not the meta data.
	if(isdatasync==0){
		return fsync(fi->fh);
	}
	
	return fdatasync(fi->fh);
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int xmp_setxattr(const char *path, const char *name, const char *value,
			size_t size, int flags)
{
	int res = lsetxattr(path, name, value, size, flags);
	if (res == -1)
		return -errno;
	return 0;
}

static int xmp_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	int res = lgetxattr(path, name, value, size);
	if (res == -1)
		return -errno;
	return res;
}

static int xmp_listxattr(const char *path, char *list, size_t size)
{
	int res = llistxattr(path, list, size);
	if (res == -1)
		return -errno;
	return res;
}

static int xmp_removexattr(const char *path, const char *name)
{
	int res = lremovexattr(path, name);
	if (res == -1)
		return -errno;
	return 0;
}
#endif /* HAVE_SETXATTR */


static off_t xmp_lseek(const char *path, off_t off, int whence, struct fuse_file_info *fi)
{
	int fd;
	off_t res;

	if (fi == NULL)
		fd = open(path, O_RDONLY);
	else
		fd = fi->fh;

	if (fd == -1)
		return -errno;

	res = lseek(fd, off, whence);
	if (res == -1)
		res = -errno;

	if (fi == NULL)
		close(fd);
	return res;
}

static const struct fuse_operations xmp_oper = {
	.init           = xmp_init,
	.destroy = xmp_destroy,
	.getattr	= xmp_getattr,
	.access		= xmp_access,
	.readlink	= xmp_readlink,
	.readdir	= xmp_readdir,
	.mknod		= xmp_mknod,
	.mkdir		= xmp_mkdir,
	.symlink	= xmp_symlink,
	.unlink		= xmp_unlink,
	.rmdir		= xmp_rmdir,
	.rename		= xmp_rename,
	.link		= xmp_link,
	.chmod		= xmp_chmod,
	.chown		= xmp_chown,
	.truncate	= xmp_truncate,
#ifdef HAVE_UTIMENSAT
	.utimens	= xmp_utimens,
#endif
	.open		= xmp_open,
	.create 	= xmp_create,
	.read		= xmp_read,
	.write		= xmp_write,
	.statfs		= xmp_statfs,
	.release	= xmp_release,
	.fsync		= xmp_fsync,
#ifdef HAVE_SETXATTR
	.setxattr	= xmp_setxattr,
	.getxattr	= xmp_getxattr,
	.listxattr	= xmp_listxattr,
	.removexattr	= xmp_removexattr,
#endif
	.lseek		= xmp_lseek,
};

int main(int argc, char *argv[])
{
	enum { MAX_ARGS = 10 };
	int i,new_argc;
	char *new_argv[MAX_ARGS];

	umask(0);
			/* Process the "--plus" option apart */
	for (i=0, new_argc=0; (i<argc) && (new_argc<MAX_ARGS); i++) {
		if (!strcmp(argv[i], "--plus")) {
			fill_dir_plus = FUSE_FILL_DIR_PLUS;
		} else if (strncmp(argv[i], "--server=", 9) == 0) {
			strncpy(server_address, argv[i] + 9, sizeof(server_address) - 1);
			server_address[sizeof(server_address) - 1] = '\0';
		} else {
			new_argv[new_argc++] = argv[i];
		}
	}

	return fuse_main(new_argc, new_argv, &xmp_oper, NULL);
}
