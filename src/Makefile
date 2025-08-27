CC = gcc
CFLAGS = -Wall
PKG_CFLAGS = `pkg-config --cflags glib-2.0 fuse3`
PKG_LIBS = `pkg-config --libs glib-2.0 fuse3`
DEPS = namespace.h remote.h metaindex.h

OBJP = passthrough.o remote.o metaindex.o
OBJMS = metaserver.o remote.o namespace.o metaindex.o
OBJDS = dataserver.o remote.o metaindex.o

all: passthrough metaserver dataserver

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS) $(PKG_CFLAGS)

passthrough: $(OBJP)
	$(CC) -o $@ $^ $(CFLAGS) $(PKG_LIBS) -lpthread -lcrypto

metaserver: $(OBJMS)
	$(CC) -o $@ $^ $(CFLAGS) $(PKG_LIBS) -lpthread -lcrypto

dataserver: $(OBJDS)
	$(CC) -o $@ $^ $(CFLAGS) $(PKG_LIBS) -lpthread -lcrypto

.PHONY: clean
clean:
	rm -rf *.o passthrough metaserver dataserver /backend/* ~/home/vagrant/server/*
