CFLAGS = $(shell pkg-config --cflags glib-2.0)
CLIBS = $(shell pkg-config --libs glib-2.0)

IDIR = ../lib
CXXFLAGS += -I$(IDIR)

# Directory di destinazione per i file oggetto
OBJ_DIR = build

# Lista dei file oggetto
OBJ_FILES = $(OBJ_DIR)/main.o

LOG_FILE = build/log.txt

all: create_build_dir $(OBJ_FILES)
	mpicc -g $(OBJ_FILES) -o Count.out $(CFLAGS) $(CLIBS) 2>&1 | tee -a $(LOG_FILE)

$(OBJ_DIR)/main.o: main.c
	mpicc -g $(CXXFLAGS) -c $< -o $@ $(CFLAGS) $(CLIBS) 2>&1 | tee -a $(LOG_FILE)

create_build_dir:
	mkdir -p $(OBJ_DIR)

clean:
	rm -rf $(OBJ_DIR) Count.out Count.txt ../*.txt

.PHONY: all clean
