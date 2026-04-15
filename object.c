// object.c — Content-addressable object store

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <openssl/evp.h>

// Convert binary hash to hex string
void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// Write an object to the store.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str = NULL;
    if (type == OBJ_BLOB) type_str = "blob";
    else if (type == OBJ_TREE) type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    char header[64];
    int h = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    if (h < 0) return -1;
    size_t header_len = (size_t)h + 1; // include null separator

    size_t full_size = header_len + len;
    uint8_t *full = malloc(full_size);
    if (!full) return -1;
    memcpy(full, header, header_len);
    if (len > 0) memcpy(full + header_len, data, len);

    ObjectID id;
    compute_hash(full, full_size, &id);

    // Deduplication check
    if (object_exists(&id)) {
        if (id_out) *id_out = id;
        free(full);
        return 0;
    }

    // Ensure shard directory exists
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    if (mkdir(shard_dir, 0755) != 0 && errno != EEXIST) {
        free(full);
        return -1;
    }

    char final_path[512];
    object_path(&id, final_path, sizeof(final_path));
    char tmp_path[560];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", final_path);

    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) { free(full); return -1; }

    size_t wrote = 0;
    while (wrote < full_size) {
        ssize_t w = write(fd, full + wrote, full_size - wrote);
        if (w <= 0) { close(fd); unlink(tmp_path); free(full); return -1; }
        wrote += (size_t)w;
    }
    fsync(fd);
    close(fd);

    if (rename(tmp_path, final_path) != 0) { unlink(tmp_path); free(full); return -1; }

    // Sync the shard directory to persist the rename
    int dfd = open(shard_dir, O_RDONLY);
    if (dfd >= 0) { fsync(dfd); close(dfd); }

    if (id_out) *id_out = id;
    free(full);
    return 0;
}

// Read an object from the store.
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    long fsize = ftell(f);
    if (fsize < 0) { fclose(f); return -1; }
    rewind(f);

    uint8_t *buf = malloc((size_t)fsize);
    if (!buf) { fclose(f); return -1; }
    if (fread(buf, 1, (size_t)fsize, f) != (size_t)fsize) { free(buf); fclose(f); return -1; }
    fclose(f);

    // Verify integrity
    ObjectID computed;
    compute_hash(buf, (size_t)fsize, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) { free(buf); return -1; }

    // Find header/data split
    uint8_t *sep = memchr(buf, '\0', (size_t)fsize);
    if (!sep) { free(buf); return -1; }
    size_t header_len = (size_t)(sep - buf) + 1;

    char type_str[16];
    size_t declared_size = 0;
    if (sscanf((char *)buf, "%15s %zu", type_str, &declared_size) != 2) { free(buf); return -1; }

    size_t data_len = (size_t)fsize - header_len;
    if (data_len != declared_size) { free(buf); return -1; }

    if (strcmp(type_str, "blob") == 0) *type_out = OBJ_BLOB;
    else if (strcmp(type_str, "tree") == 0) *type_out = OBJ_TREE;
    else if (strcmp(type_str, "commit") == 0) *type_out = OBJ_COMMIT;
    else { free(buf); return -1; }

    void *out = malloc(data_len);
    if (!out) { free(buf); return -1; }
    memcpy(out, buf + header_len, data_len);
    *data_out = out;
    *len_out = data_len;

    free(buf);
    return 0;
}
