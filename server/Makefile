# MINIDBMS Server Makefile

CC      = gcc
CFLAGS  = -Wall -Wextra -g -I..
STORAGE = ../storage-engine
DATA_DIR ?= ../data
NUM_FRAMES ?= 64
POLICY ?= lru

# All storage engine object files needed
STORAGE_OBJS = \
    $(STORAGE)/buffer_manager.o \
    $(STORAGE)/eviction_policy.o \
    $(STORAGE)/page_table.o \
    $(STORAGE)/buffer_frame.o \
    $(STORAGE)/heap.o \
    $(STORAGE)/page.o \
    $(STORAGE)/disk.o \
    $(STORAGE)/schema.o \
    $(STORAGE)/trace.o

# Server object files
SERVER_OBJS = \
    server.o \
    protocol.o \
    handlers.o

# ============================================================================
# Main target
# ============================================================================

all: minidbms-server

minidbms-server: $(SERVER_OBJS) $(STORAGE_OBJS)
	$(CC) $(CFLAGS) -o minidbms-server $(SERVER_OBJS) $(STORAGE_OBJS)
	@echo "Built: minidbms-server"

# ============================================================================
# Server objects
# ============================================================================

server.o: server.c server.h protocol.h handlers.h
	$(CC) $(CFLAGS) -c server.c -o server.o

protocol.o: protocol.c protocol.h
	$(CC) $(CFLAGS) -c protocol.c -o protocol.o

handlers.o: handlers.c handlers.h server.h protocol.h
	$(CC) $(CFLAGS) -c handlers.c -o handlers.o

# ============================================================================
# Storage engine objects
# ============================================================================

$(STORAGE)/buffer_manager.o: $(STORAGE)/buffer_manager.c
	$(CC) $(CFLAGS) -c $(STORAGE)/buffer_manager.c -o $(STORAGE)/buffer_manager.o

$(STORAGE)/eviction_policy.o: $(STORAGE)/eviction_policy.c
	$(CC) $(CFLAGS) -c $(STORAGE)/eviction_policy.c -o $(STORAGE)/eviction_policy.o

$(STORAGE)/page_table.o: $(STORAGE)/page_table.c
	$(CC) $(CFLAGS) -c $(STORAGE)/page_table.c -o $(STORAGE)/page_table.o

$(STORAGE)/buffer_frame.o: $(STORAGE)/buffer_frame.c
	$(CC) $(CFLAGS) -c $(STORAGE)/buffer_frame.c -o $(STORAGE)/buffer_frame.o

$(STORAGE)/heap.o: $(STORAGE)/heap.c
	$(CC) $(CFLAGS) -c $(STORAGE)/heap.c -o $(STORAGE)/heap.o

$(STORAGE)/page.o: $(STORAGE)/page.c
	$(CC) $(CFLAGS) -c $(STORAGE)/page.c -o $(STORAGE)/page.o

$(STORAGE)/disk.o: $(STORAGE)/disk.c
	$(CC) $(CFLAGS) -c $(STORAGE)/disk.c -o $(STORAGE)/disk.o

$(STORAGE)/schema.o: $(STORAGE)/schema.c
	$(CC) $(CFLAGS) -c $(STORAGE)/schema.c -o $(STORAGE)/schema.o

$(STORAGE)/trace.o: $(STORAGE)/trace.c $(STORAGE)/trace.h
	$(CC) $(CFLAGS) -c $(STORAGE)/trace.c -o $(STORAGE)/trace.o

# ============================================================================
# Utilities
# ============================================================================

clean:
	rm -f $(SERVER_OBJS) $(STORAGE_OBJS) minidbms-server
	@echo "Cleaned"

run: minidbms-server
	mkdir -p $(DATA_DIR)
	./minidbms-server $(DATA_DIR) $(NUM_FRAMES) $(POLICY)

.PHONY: all clean run
