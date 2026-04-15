// index.c — Staging area implementation

#include "index.h"
#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <time.h>

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0) memmove(&index->entries[i], &index->entries[i + 1], remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");

    printf("\n");
    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec || st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");

    printf("\n");
    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;

            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) { is_tracked = 1; break; }
            }
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) { printf("  untracked:  %s\n", ent->d_name); untracked_count++; }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// Load the index from .pes/index.
int index_load(Index *index) {
    index->count = 0;
    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) {
        // No index is not an error; initialize empty index
        index->count = 0;
        return 0;
    }

    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        // Strip newline
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '\0') continue;
        unsigned int mode = 0;
        char hex[HASH_HEX_SIZE + 1];
        unsigned long long mtime = 0;
        unsigned int size = 0;
        char path[512];
        int rc = sscanf(line, "%o %64s %llu %u %511[^\n]", &mode, hex, &mtime, &size, path);
        if (rc == 5) {
            if (index->count >= MAX_INDEX_ENTRIES) { fclose(f); return -1; }
            index->entries[index->count].mode = mode;
            if (hex_to_hash(hex, &index->entries[index->count].hash) != 0) { fclose(f); return -1; }
            index->entries[index->count].mtime_sec = (uint64_t)mtime;
            index->entries[index->count].size = size;
            strncpy(index->entries[index->count].path, path, sizeof(index->entries[index->count].path) - 1);
            index->entries[index->count].path[sizeof(index->entries[index->count].path)-1] = '\0';
            index->count++;
        }
    }
    fclose(f);
    return 0;
}

static int cmp_index_entries(const void *a, const void *b) {
    const IndexEntry *ea = a;
    const IndexEntry *eb = b;
    return strcmp(ea->path, eb->path);
}

int index_save(const Index *index) {
    // Make a copy to sort
    Index tmp = *index;
    qsort(tmp.entries, tmp.count, sizeof(IndexEntry), cmp_index_entries);

    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", INDEX_FILE);
    FILE *f = fopen(tmp_path, "w");
    if (!f) return -1;
    for (int i = 0; i < tmp.count; i++) {
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&tmp.entries[i].hash, hex);
        fprintf(f, "%o %s %llu %u %s\n", tmp.entries[i].mode, hex, (unsigned long long)tmp.entries[i].mtime_sec, tmp.entries[i].size, tmp.entries[i].path);
    }
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    if (rename(tmp_path, INDEX_FILE) != 0) { unlink(tmp_path); return -1; }
    return 0;
}

int index_add(Index *index, const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) { fprintf(stderr, "error: cannot stat '%s'\n", path); return -1; }
    if (!S_ISREG(st.st_mode)) { fprintf(stderr, "error: '%s' is not a regular file\n", path); return -1; }

    FILE *f = fopen(path, "rb");
    if (!f) { fprintf(stderr, "error: cannot open '%s'\n", path); return -1; }
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    long fsize = ftell(f);
    if (fsize < 0) { fclose(f); return -1; }
    rewind(f);
    uint8_t *buf = malloc((size_t)fsize);
    if (!buf) { fclose(f); return -1; }
    if (fread(buf, 1, (size_t)fsize, f) != (size_t)fsize) { free(buf); fclose(f); return -1; }
    fclose(f);

    ObjectID id;
    if (object_write(OBJ_BLOB, buf, (size_t)fsize, &id) != 0) { free(buf); return -1; }
    free(buf);

    IndexEntry *ent = index_find(index, path);
    uint32_t mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;
    if (ent) {
        ent->mode = mode;
        ent->hash = id;
        ent->mtime_sec = (uint64_t)st.st_mtime;
        ent->size = (uint32_t)st.st_size;
    } else {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        ent = &index->entries[index->count];
        ent->mode = mode;
        ent->hash = id;
        ent->mtime_sec = (uint64_t)st.st_mtime;
        ent->size = (uint32_t)st.st_size;
        strncpy(ent->path, path, sizeof(ent->path)-1);
        ent->path[sizeof(ent->path)-1] = '\0';
        index->count++;
    }

    return index_save(index);
}
