#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>


#include <gf_complete.h>
#include "Util/jerasure.h"
#include "Util/reed_sol.h"

#define MAX_CMD_LEN 512
#define MAX_FILENAME_LEN 256

typedef struct {
    int start;
    int len;
} azure_lrc_grp_idx;

typedef struct {
    int k; // number of data fragments
    int l; // number of local groups. k % l == 0
    int r; // number of global parities
    int n; // n = k + l + r
    int m; // m = l + r
    int grp_size; // number of data fragments per local group
    azure_lrc_grp_idx *grp_idxes; // local groups
    int *matrix; // encoding matrix (l + r, k)
    int w; // GF(2^w)
} azure_lrc_params;

typedef struct {
    int num_d; // number of data fragments
    int num_c; // number of codes
    int n; // n = num_d + num_c
    int64_t chunk_size; // chunk size

    char *buf;
    char **data;
    char **code;
} azure_lrc_buf;


int azure_lrc_init(azure_lrc_params *lrc, int k, int l, int r);
int *azure_lrc_gen_matrix(azure_lrc_params *lrc);
int azure_lrc_init_buf(azure_lrc_buf *buf, azure_lrc_params *lrc, int64_t chunk_size);
int azure_lrc_encode(azure_lrc_params *lrc, azure_lrc_buf *buf);
int azure_lrc_decode(azure_lrc_params *lrc, azure_lrc_buf *buf, int *erased);
int azure_lrc_get_survivors(azure_lrc_params *lrc, int *erased, int *survivors);
void azure_lrc_destroy(azure_lrc_params *lrc, azure_lrc_buf *buf);


int main(int argc, char * argv[]) {
    if (argc != 5) {
        //printf("Usage: ./%s blocksizeMB[64] ecn[10] eck[6] ecl[2] ecg[2]\n", __FILE__);
        printf("Usage: ./%s blocksizeMB[64] ecn[10] eck[6] ecr[3]\n", __FILE__);
        return 0;
    }

    int blocksizeMB = atoi(argv[1]);
    int ecn = atoi(argv[2]);
    int eck = atoi(argv[3]);
    int ecr = atoi(argv[4]);
    //int ecl = atoi(argv[4]);
    //int ecg = atoi(argv[5]);

    int ecl = eck/ecr;
    int ecg = ecn-eck-ecl;
    int ecm = ecl + ecg;
    long block_size = blocksizeMB * 1048576;
    long chunk_size = 1048576; // 1MB buffer
    int num_chunks_per_block = block_size / chunk_size;
    const char *filename_prefix = "blk_";
    char cmd[MAX_CMD_LEN];
    char filename[MAX_FILENAME_LEN];
    
    if (ecn != eck + ecm) {
        printf("error: invalid azure_lrc params (%d,%d,%d,%d)\n", ecn, eck, ecl, ecg);
        return -1;
    }

    if (blocksizeMB < 1) {
        printf("error: invalid blocksizeMB %d\n", blocksizeMB);
        return -1;
    }

    // init lrc
    azure_lrc_params lrc;
    azure_lrc_buf buf;

    if (azure_lrc_init(&lrc, eck, ecl, ecg) < 0) {
        printf("error: azure_lrc_init\n");
        azure_lrc_destroy(&lrc, &buf);
        return 0;
    }

    if (azure_lrc_init_buf(&buf, &lrc, chunk_size) < 0) {
        printf("error: azure_lrc_init_buf\n");
        azure_lrc_destroy(&lrc, &buf);
        return 0;
    }

    // generate data blocks by linux dd
    for (int data_blk_id = 0; data_blk_id < eck; data_blk_id++) {
        memset(cmd, 0, MAX_CMD_LEN * sizeof(char));
        snprintf(cmd, MAX_CMD_LEN, "dd if=/dev/urandom of=%s%d iflag=fullblock bs=%ld count=%d", 
            filename_prefix, data_blk_id, chunk_size, blocksizeMB);
        if (system(cmd) == -1) {
            printf("error: failed to generate data block %d\n", data_blk_id);
            return -1;
        }
        printf("complete generate data block %d\n", data_blk_id);
    }

    for (int chunk_id = 0; chunk_id < num_chunks_per_block; chunk_id++) {
        // read data chunks
        for (int data_blk_id = 0; data_blk_id < eck; data_blk_id++) {
            memset(filename, 0, MAX_FILENAME_LEN * sizeof(char));
            snprintf(filename, MAX_FILENAME_LEN, "%s%d", filename_prefix, data_blk_id);

            int fd = open(filename, O_RDONLY);
            lseek(fd, chunk_id * chunk_size, SEEK_SET);

            long bytes_read = 0;
            while (bytes_read < chunk_size) {
                bytes_read += read(fd, buf.data[data_blk_id] + bytes_read, chunk_size);
            }

            close(fd);
        }
        
        // azure_lrc_encode
        if (azure_lrc_encode(&lrc, &buf) < 0) {
            printf("error: azure_lrc_encode for chunk %d\n", chunk_id);
            azure_lrc_destroy(&lrc, &buf);
            return 0;
        }

        // write parity chunks
        for (int parity_blk_id = 0; parity_blk_id < ecm; parity_blk_id++) {
            memset(filename, 0, MAX_FILENAME_LEN * sizeof(char));
            snprintf(filename, MAX_FILENAME_LEN, "%s%d", filename_prefix, eck + parity_blk_id);
            int fd = open(filename, O_RDWR | O_CREAT, 0644);
            lseek(fd, chunk_id * chunk_size, SEEK_SET);

            long bytes_write = 0;
            while (bytes_write < chunk_size) {
                bytes_write += write(fd, buf.code[parity_blk_id] + bytes_write, chunk_size);
            }
            close(fd);
        }

        // printf("complete generate chunk %d\n", chunk_id);
    }

    printf("complete generating blocks\n");

    return 0;
}

int azure_lrc_init(azure_lrc_params *lrc, int k, int l, int r) {
    if (l > k || r > k) {
        printf("error: invalid (k, l, r) == (%d, %d, %d) \n", k, l, r);
        return -1;
    }

    if (k % l != 0) {
        printf("error: k %% r == %d %% %d != 0\n", k, l);
        return -1;
    }

    lrc->k = k;
    lrc->l = l;
    lrc->r = r;
    lrc->m = lrc->l + lrc->r;
    lrc->n = lrc->k + lrc->m;
    lrc->grp_size = lrc->k / lrc->l;

    lrc->w = 8;

    printf("azure_lrc_params: (k, l, r) = (%d, %d, %d)\n", lrc->k, lrc->l, lrc->r);

    lrc->grp_idxes = (azure_lrc_grp_idx *) malloc(lrc->l * sizeof(azure_lrc_grp_idx));
    for (int i = 0; i < lrc->l; i++) {
        lrc->grp_idxes[i].start = i * lrc->grp_size;
        lrc->grp_idxes[i].len = lrc->grp_size;
    }

    lrc->matrix = azure_lrc_gen_matrix(lrc);
    if (lrc->matrix == NULL) {
        printf("error: init lrc_matrix\n");
        azure_lrc_destroy(lrc, NULL);
        return -1;
    }

    return 0;
}

int *azure_lrc_gen_matrix(azure_lrc_params *lrc) {

    int *vm_matrix = NULL;

    // generate vm matrix for global parities
    // note that the first row will be all 1s, which we don't need
    vm_matrix = reed_sol_vandermonde_coding_matrix(lrc->k, lrc->r + 1, 8);

    if (vm_matrix == NULL) {
        return NULL;
    }

    // printf("init vm_matrix with %d, %d\n", lrc->r + 1, lrc->k);

    // for (int i = 0; i < lrc->r + 1; i++) {
    //     for (int j = 0; j < lrc->k; j++) {
    //         printf("%d ", vm_matrix[i * lrc->k + j]);
    //     }
    //     printf("\n");
    // }
    // printf("\n");


    // generate lrc matrix
    int *lrc_matrix = (int*)calloc(lrc->k * lrc->m, sizeof(int));
    if (lrc_matrix == NULL) {
        azure_lrc_destroy(lrc, NULL);
        return NULL;
    }

    /********************************/
    /* 110000
       001100
       000011
       vm_matrix[1, r]              */
    /********************************/

    // local
    for (int i = 0; i < lrc->l; i++) {
        azure_lrc_grp_idx *grp_idx = &lrc->grp_idxes[i];
        for (int j = 0; j < grp_idx->len; j++) {
            lrc_matrix[i * lrc->k + grp_idx->start + j] = 1;
        }
    }

    // copy from row 1 in vm_matrix
    for (int i = 0; i < lrc->r; i++) {
        for (int j = 0; j < lrc->k; j++) {
            lrc_matrix[(lrc->l + i) * lrc->k + j] = vm_matrix[(i + 1) * lrc->k + j];
        }
    }

    printf("init lrc_matrix with %d, %d: \n", lrc->m, lrc->k);
    for (int i = 0; i < lrc->m; i++) {
        for (int j = 0; j < lrc->k; j++) {
            printf("%d ", lrc_matrix[i * lrc->k + j]);
        }
        printf("\n");
    }
    printf("\n");

    free(vm_matrix);

    return lrc_matrix;
}

int azure_lrc_init_buf(azure_lrc_buf *buf, azure_lrc_params *lrc, int64_t chunk_size) {
    buf->num_d = lrc->k;
    buf->num_c = lrc->m;
    buf->n = buf->num_d + buf->num_c;
    buf->chunk_size = chunk_size;

    // allocate buffer for n chunks
    buf->buf = (char *)calloc(buf->n * chunk_size, sizeof(char));

    if (buf->buf == NULL) {
        printf("error: allocate buffer\n");
        return -1;
    }

    // init pointers
    buf->data = (char **)malloc(buf->num_d * sizeof(char *));
    if (buf->data == NULL) {
        printf("error: allocate data buffer\n");
        azure_lrc_destroy(lrc, buf);
        return -1;
    }
    for (int i = 0; i < buf->num_d; i++) {
        buf->data[i] = buf->buf + chunk_size * i;
    }
    buf->code = (char **)malloc(buf->num_c * sizeof(char *));
    if (buf->data == NULL) {
        printf("error: allocate code buffer\n");
        azure_lrc_destroy(lrc, buf);
        return -1;
    }
    for (int i = 0; i < buf->num_c; i++) {
        buf->code[i] = buf->buf + chunk_size * (i + buf->num_d);
    }

    return 0;
}

int azure_lrc_encode(azure_lrc_params *lrc, azure_lrc_buf *buf) {
    // call jerasure encode
    jerasure_matrix_encode(lrc->k, lrc->m, lrc->w, lrc->matrix, buf->data, buf->code, buf->chunk_size);

    return 0;
}

int azure_lrc_decode(azure_lrc_params *lrc, azure_lrc_buf *buf, int *erased) {
    int *survivors = (int *)calloc(lrc->n, sizeof(int));

    if (azure_lrc_get_survivors(lrc, erased, survivors) < 0) {
        printf("error: failed pattern for decode\n");
        free(survivors);
        return -1;
    }

    // printf("suvivors:\n");
    // for (int i = 0; i < lrc->n; i++) {
    //     printf("%d ", survivors[i]);
    // }
    // printf("\n");

    int *decode_matrix = (int *)calloc(lrc->m * lrc->k, sizeof(int));

    // IMPORTANT: for jerasure decode, remap codes, buffer
    int *jerasure_erased = (int *)calloc(lrc->n, sizeof(int));
    char **jerasure_code_buf = (char **)calloc(lrc->m, sizeof(char *));
    for (int i = 0; i < lrc->m; i++) {
        jerasure_code_buf[i] = buf->code[i];
    }

    // only map data part to erased, code parts will be remapped
    for (int i = 0; i < lrc->k; i++) {
        jerasure_erased[i] = erased[i];
    }

    int remap_to_idx = lrc->k;
    for (int i = lrc->k; i < lrc->n; i++) {
        if (survivors[i] == 1 || erased[i] == 1) {
            // printf("decode: remap: %d -> %d\n", i, remap_to_idx);
            jerasure_code_buf[remap_to_idx - lrc->k] = buf->code[i - lrc->k];
            jerasure_erased[remap_to_idx] = erased[i];

            // copy the parity rows of generator matrix to the front
            for (int j = 0; j < lrc->k; j++) {
                decode_matrix[(remap_to_idx - lrc->k) * lrc->k + j] = lrc->matrix[(i - lrc->k) * lrc->k + j];
            }

            remap_to_idx++;
        }
    }

    // printf("jerasure_erased:\n");
    // for (int i = 0; i < lrc->n; i++) {
    //     printf("%d ", jerasure_erased[i]);
    // }
    // printf("\n");


    // printf("decode_matrix: \n");
    // for (int i = 0; i < lrc->m; i++) {
    //     for (int j = 0; j < lrc->k; j++) {
    //         printf("%d ", decode_matrix[i * lrc->k + j]);
    //     }
    //     printf("\n");
    // }
    // printf("\n");

    int *erasures = (int *)calloc(lrc->n, sizeof(int));
    int lastidx = 0;

    for (int i = 0; i < lrc->n; i++) {
        if (jerasure_erased[i] == 1) {
            erasures[lastidx] = i;
            lastidx++;
        }
    }
    erasures[lastidx] = -1;

    int jerasure_decode_m = remap_to_idx - lrc->k;

    if (jerasure_matrix_decode(lrc->k, jerasure_decode_m, lrc->w, decode_matrix, 0,
                erasures, buf->data, jerasure_code_buf,
                buf->chunk_size) < 0) {
        printf("error: jerasure_matrix_decode\n");
        free(survivors);
        free(erasures);
        azure_lrc_destroy(lrc, buf);

        return -1;
    }

    // decode matrix will be freed by jerasure_matrix_decode
    free(jerasure_code_buf);
    free(jerasure_erased);
    free(survivors);
    free(erasures);

    return 0;

}

int azure_lrc_get_survivors(azure_lrc_params *lrc, int *erased, int *survivors) {

    // local group
    for (int grp_idx = 0; grp_idx < lrc->l; grp_idx++) {
        int start = lrc->grp_idxes[grp_idx].start;
        int num_failed = 0;
        // data chunks
        for (int j = 0; j < lrc->grp_idxes[grp_idx].len; j++) {
            if (erased[start + j] == 1) {
                num_failed++;
            }
        }
        // local parity
        if (erased[grp_idx + lrc->k] == 1) {
            num_failed++;
        }

        // no failed chunk in local group
        if (num_failed == 0) {
            continue;
        }

        // printf("group %d, num_failed: %d\n", i, num_failed);

        if (num_failed == 1) {
            for (int j = 0; j < lrc->grp_idxes[grp_idx].len; j++) {
                if (erased[start + j] == 0) {
                    survivors[start + j] = 1;
                }
            }

            if (erased[grp_idx + lrc->k] == 0) {
                // use local parity to decode
                survivors[grp_idx + lrc->k] = 1;
            }

            // printf("group %d, local repair can be enabled\n", grp_idx);

            // move to next local group
            continue;
        }

        // use available chunks to decode
        for (int j = 0; j < lrc->grp_idxes[grp_idx].len; j++) { // data chunks
            if (erased[start + j] == 0) {
                survivors[start + j] = 1;
            }
        }
        if (erased[lrc->k + grp_idx] == 0) { // local parity
            survivors[lrc->k + grp_idx] = 1;
            num_failed--;
        }
        for (int j = 0; j < lrc->r; j++) {
            if (erased[lrc->k + lrc->l + j] == 0 && survivors[lrc->k + lrc->l + j] == 0) {
                survivors[lrc->k + lrc->l + j] = 1;
                num_failed--;

                if (num_failed <= 0) {
                    break;
                }
            }
        }

        if (num_failed > 0) {
            printf("local group %d decode: failed pattern, lost too many chunks\n", grp_idx);
            return -1;
        }
    }


    // global parity
    for (int i = 0; i < lrc->r; i++) {
        if (erased[lrc->k + lrc->l + i] == 0) {
            continue;
        }

        // use data + global parity to decode
        int num_survivors = 0;
        for (int j = 0; j < lrc->k; j++) {
            if (erased[j] == 0) {
                num_survivors++;
            }
        }
        for (int j = 0; j < lrc->r; j++) {
            if (erased[lrc->k + lrc->l + j] == 0) {
                num_survivors++;
            }
        }

        if (num_survivors < lrc->k) {
            printf("global decode: failed pattern, lost too many chunks\n");
            return -1;
        }

        // pick k chunks
        int count = 0;
        for (int j = 0; j < lrc->k; j++) {
            if (erased[j] == 0) {
                survivors[j] = 1;
                count++;
            }
        }

        for (int j = 0; j < lrc->r; j++) {
            if (count < lrc->k) {
                survivors[lrc->k + lrc->l + j] = 1;
                count++;
            } else {
                break;
            }
        }

        if (count >= lrc->k) {
            break;
        }
    }

    return 0;
}

void azure_lrc_destroy(azure_lrc_params *lrc, azure_lrc_buf *buf) {
    if (lrc != NULL) {
        if (lrc->grp_idxes != NULL) {
            free(lrc->grp_idxes);
        }
        if (lrc->matrix != NULL) {
            free(lrc->matrix);
        }
    }

    if (buf != NULL) {
        if (buf->data != NULL) {
            free(buf->data);
        }
        if (buf->code != NULL) {
            free(buf->code);
        }
        if (buf->buf != NULL) {
            free(buf->buf);
        }
    }
}

