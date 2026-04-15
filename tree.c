// tree.c — Tree object serialization and construction

#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;
    if (S_ISDIR(st.st_mode)) return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1;
        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);
        ptr = space + 1;

        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1;
        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0';
        ptr = null_byte + 1;

        if (ptr + HASH_SIZE > end) return -1;
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    size_t max_size = tree->count * 296;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1; // include null
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// Recursive helper: prefix is either "" (root) or "dir/" with trailing slash
static int write_tree_level(const Index *index, const char *prefix, ObjectID *id_out) {
    Tree tree;
    tree.count = 0;
    size_t prefix_len = prefix ? strlen(prefix) : 0;

    for (int i = 0; i < index->count; i++) {
        const char *path = index->entries[i].path;
        if (prefix_len > 0) {
            if (strncmp(path, prefix, prefix_len) != 0) continue;
        }
        const char *rest = path + prefix_len;
        const char *slash = strchr(rest, '/');

        if (!slash) {
            // file in this directory
            if (tree.count >= MAX_TREE_ENTRIES) return -1;
            tree.entries[tree.count].mode = index->entries[i].mode;
            tree.entries[tree.count].hash = index->entries[i].hash;
            strncpy(tree.entries[tree.count].name, rest, sizeof(tree.entries[tree.count].name) - 1);
            tree.entries[tree.count].name[sizeof(tree.entries[tree.count].name)-1] = '\0';
            tree.count++;
        } else {
            // subdirectory
            char name[256];
            size_t name_len = (size_t)(slash - rest);
            if (name_len >= sizeof(name)) return -1;
            memcpy(name, rest, name_len);
            name[name_len] = '\0';

            // Check if already added
            int found = 0;
            for (int j = 0; j < tree.count; j++) {
                if (tree.entries[j].mode == MODE_DIR && strcmp(tree.entries[j].name, name) == 0) { found = 1; break; }
            }
            if (found) continue;

            // Build next prefix
            char next_prefix[512];
            if (prefix_len == 0) snprintf(next_prefix, sizeof(next_prefix), "%s/", name);
            else snprintf(next_prefix, sizeof(next_prefix), "%s%s/", prefix, name);

            ObjectID child_id;
            if (write_tree_level(index, next_prefix, &child_id) != 0) return -1;

            if (tree.count >= MAX_TREE_ENTRIES) return -1;
            tree.entries[tree.count].mode = MODE_DIR;
            tree.entries[tree.count].hash = child_id;
            strncpy(tree.entries[tree.count].name, name, sizeof(tree.entries[tree.count].name)-1);
            tree.entries[tree.count].name[sizeof(tree.entries[tree.count].name)-1] = '\0';
            tree.count++;
        }
    }

    // Serialize and write this tree object
    void *data;
    size_t len;
    if (tree_serialize(&tree, &data, &len) != 0) return -1;
    if (object_write(OBJ_TREE, data, len, id_out) != 0) { free(data); return -1; }
    free(data);
    return 0;
}

int tree_from_index(ObjectID *id_out) {
    Index index;
    if (index_load(&index) != 0) return -1;
    // start from root with empty prefix
    return write_tree_level(&index, "", id_out);
}
