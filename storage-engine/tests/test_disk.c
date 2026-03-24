// test_disk.c — Test exhaustivo del módulo disk
// Compilar: gcc -Wall -Wextra -g -o test_disk test_disk.c ../storage-engine/disk.c
// Ejecutar: ./test_disk

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../disk.h"

// ============================================================================
// Infraestructura
// ============================================================================

static int tests_passed = 0;
static int tests_failed  = 0;
static char TEST_DIR[64];
static char TEST_DIR_GLOBAL[64]; // para helpers con fork

#define ASSERT(cond, msg) do { \
    if (!(cond)) { \
        printf("    FAIL: %s (linea %d)\n", msg, __LINE__); \
        tests_failed++; \
        return 0; \
    } \
} while(0)

#define ASSERT_INT(expected, actual, msg) do { \
    if ((expected) != (actual)) { \
        printf("    FAIL: %s — esperado %d, obtenido %d (linea %d)\n", \
               msg, expected, actual, __LINE__); \
        tests_failed++; \
        return 0; \
    } \
} while(0)

#define ASSERT_MEM(expected, actual, size, msg) do { \
    if (memcmp(expected, actual, size) != 0) { \
        printf("    FAIL: %s — buffers difieren (linea %d)\n", msg, __LINE__); \
        tests_failed++; \
        return 0; \
    } \
} while(0)

#define ASSERT_BYTE(expected, actual, pos, msg) do { \
    if ((unsigned char)(expected) != (unsigned char)(actual)) { \
        printf("    FAIL: %s — pos %d: esperado 0x%02X, obtenido 0x%02X (linea %d)\n", \
               msg, pos, (unsigned char)(expected), (unsigned char)(actual), __LINE__); \
        tests_failed++; \
        return 0; \
    } \
} while(0)

#define RUN_TEST(fn) do { \
    printf("  %-65s", #fn); \
    fflush(stdout); \
    cleanup_files(); \
    int _r = fn(); \
    if (_r) { printf("PASS\n"); tests_passed++; } \
    else    { printf("(ver arriba)\n"); } \
} while(0)

static void cleanup_files(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -f %s/*.db 2>/dev/null", TEST_DIR);
    system(cmd);
}

// Ejecuta una función en un proceso hijo y retorna 1 si ese proceso
// termina con código de salida != 0 (es decir, llamó a exit(1))
static int exits_with_failure(void (*fn)(void)) {
    pid_t pid = fork();
    if (pid == 0) {
        freopen("/dev/null", "w", stderr);
        fn();
        exit(0); // no debería llegar aquí si fn() llama exit(1)
    }
    int status;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) && WEXITSTATUS(status) != 0;
}

// Helpers para los casos que hacen exit(1) — necesitan ser funciones globales
static void _helper_load_nonexistent(void) {
    char buf[PAGE_SIZE];
    load_page(TEST_DIR_GLOBAL, "tabla_que_no_existe", 0, buf);
}

static void _helper_load_out_of_bounds(void) {
    char buf[PAGE_SIZE];
    // Escribe solo página 0, intenta leer página 1
    char page[PAGE_SIZE];
    memset(page, 0xAA, PAGE_SIZE);
    write_page(TEST_DIR_GLOBAL, "oob_table", 0, page);
    load_page(TEST_DIR_GLOBAL, "oob_table", 1, buf);
}

static void _helper_load_page_2_of_1(void) {
    char buf[PAGE_SIZE];
    char page[PAGE_SIZE];
    memset(page, 0xBB, PAGE_SIZE);
    write_page(TEST_DIR_GLOBAL, "p2of1", 0, page);
    write_page(TEST_DIR_GLOBAL, "p2of1", 1, page);
    // Solo hay 2 páginas (0 y 1), intentar leer página 2 debe fallar
    load_page(TEST_DIR_GLOBAL, "p2of1", 2, buf);
}


// ============================================================================
// BLOQUE 1: get_num_pages
// ============================================================================

static int test_num_pages_no_file(void) {
    ASSERT_INT(0, get_num_pages(TEST_DIR, "no_existe_jamas"), "archivo inexistente retorna 0");
    return 1;
}

static int test_num_pages_after_one_write(void) {
    char buf[PAGE_SIZE];
    memset(buf, 0xAA, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, buf);
    ASSERT_INT(1, get_num_pages(TEST_DIR, "t"), "1 pagina escrita = get_num_pages retorna 1");
    return 1;
}

static int test_num_pages_after_three_writes(void) {
    char buf[PAGE_SIZE];
    memset(buf, 0x11, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, buf);
    write_page(TEST_DIR, "t", 1, buf);
    write_page(TEST_DIR, "t", 2, buf);
    ASSERT_INT(3, get_num_pages(TEST_DIR, "t"), "3 paginas escritas = get_num_pages retorna 3");
    return 1;
}

static int test_num_pages_with_gap(void) {
    char buf[PAGE_SIZE];
    memset(buf, 0x55, PAGE_SIZE);
    // Escribir solo página 0 y página 3 — hay hueco en 1 y 2
    write_page(TEST_DIR, "t", 0, buf);
    write_page(TEST_DIR, "t", 3, buf);
    // El archivo tiene 4 * PAGE_SIZE bytes: páginas 1 y 2 son huecos del SO
    ASSERT_INT(4, get_num_pages(TEST_DIR, "t"), "escritura con hueco: get_num_pages retorna 4");
    return 1;
}

static int test_num_pages_same_table_twice(void) {
    char buf[PAGE_SIZE];
    memset(buf, 0x77, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, buf);
    int n1 = get_num_pages(TEST_DIR, "t");
    write_page(TEST_DIR, "t", 1, buf);
    int n2 = get_num_pages(TEST_DIR, "t");
    ASSERT_INT(1, n1, "get_num_pages despues de 1 escritura");
    ASSERT_INT(2, n2, "get_num_pages despues de 2 escrituras");
    return 1;
}

static int test_num_pages_two_tables_independent(void) {
    char buf[PAGE_SIZE];
    memset(buf, 0x33, PAGE_SIZE);
    write_page(TEST_DIR, "users",  0, buf);
    write_page(TEST_DIR, "users",  1, buf);
    write_page(TEST_DIR, "users",  2, buf);
    write_page(TEST_DIR, "orders", 0, buf);
    ASSERT_INT(3, get_num_pages(TEST_DIR, "users"),  "users tiene 3 paginas");
    ASSERT_INT(1, get_num_pages(TEST_DIR, "orders"), "orders tiene 1 pagina");
    return 1;
}

static int test_num_pages_overwrite_doesnt_grow(void) {
    char buf[PAGE_SIZE];
    memset(buf, 0xAA, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, buf);
    write_page(TEST_DIR, "t", 1, buf);
    ASSERT_INT(2, get_num_pages(TEST_DIR, "t"), "2 paginas antes de overwrite");
    // Sobrescribir página 0 — no debe crecer el archivo
    memset(buf, 0xBB, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, buf);
    ASSERT_INT(2, get_num_pages(TEST_DIR, "t"), "overwrite no aumenta num_pages");
    return 1;
}


// ============================================================================
// BLOQUE 2: write_page + load_page — roundtrip básico
// ============================================================================

static int test_roundtrip_zeros(void) {
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    memset(write_buf, 0x00, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, write_buf);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(write_buf, read_buf, PAGE_SIZE, "roundtrip con todos los bytes a 0x00");
    return 1;
}

static int test_roundtrip_ff(void) {
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    memset(write_buf, 0xFF, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, write_buf);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(write_buf, read_buf, PAGE_SIZE, "roundtrip con todos los bytes a 0xFF");
    return 1;
}

static int test_roundtrip_pattern_ab(void) {
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    memset(write_buf, 0xAB, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, write_buf);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(write_buf, read_buf, PAGE_SIZE, "roundtrip con patron 0xAB");
    return 1;
}

static int test_roundtrip_cyclic_pattern(void) {
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    for (int i = 0; i < PAGE_SIZE; i++)
        write_buf[i] = (char)(i % 256);
    write_page(TEST_DIR, "t", 0, write_buf);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(write_buf, read_buf, PAGE_SIZE, "roundtrip con patron ciclico i%256");
    return 1;
}

static int test_roundtrip_full_4096_bytes(void) {
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    // Patrón que usa todos los valores de byte
    for (int i = 0; i < PAGE_SIZE; i++)
        write_buf[i] = (char)((i * 7 + 13) % 256);
    write_page(TEST_DIR, "t", 0, write_buf);
    memset(read_buf, 0x00, PAGE_SIZE);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(write_buf, read_buf, PAGE_SIZE, "roundtrip con 4096 bytes, patron complejo");
    return 1;
}

static int test_roundtrip_first_and_last_byte(void) {
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    memset(write_buf, 0x00, PAGE_SIZE);
    write_buf[0]           = (char)0xDE;
    write_buf[PAGE_SIZE-1] = (char)0xAD;
    write_page(TEST_DIR, "t", 0, write_buf);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_BYTE(0xDE, read_buf[0],           0,           "primer byte es 0xDE");
    ASSERT_BYTE(0xAD, read_buf[PAGE_SIZE-1], PAGE_SIZE-1, "ultimo byte es 0xAD");
    return 1;
}

static int test_write_creates_file(void) {
    char path[256];
    snprintf(path, sizeof(path), "%s/newfile.db", TEST_DIR);
    // El archivo no existe todavía
    ASSERT(access(path, F_OK) != 0, "archivo no existe antes de write_page");
    char buf[PAGE_SIZE];
    memset(buf, 0x11, PAGE_SIZE);
    write_page(TEST_DIR, "newfile", 0, buf);
    ASSERT(access(path, F_OK) == 0, "archivo existe despues de write_page");
    return 1;
}

static int test_roundtrip_multiple_pages(void) {
    char bufs[3][PAGE_SIZE], reads[3][PAGE_SIZE];
    memset(bufs[0], 0xAA, PAGE_SIZE);
    memset(bufs[1], 0xBB, PAGE_SIZE);
    memset(bufs[2], 0xCC, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, bufs[0]);
    write_page(TEST_DIR, "t", 1, bufs[1]);
    write_page(TEST_DIR, "t", 2, bufs[2]);
    load_page(TEST_DIR, "t", 0, reads[0]);
    load_page(TEST_DIR, "t", 1, reads[1]);
    load_page(TEST_DIR, "t", 2, reads[2]);
    ASSERT_MEM(bufs[0], reads[0], PAGE_SIZE, "pagina 0 contiene 0xAA");
    ASSERT_MEM(bufs[1], reads[1], PAGE_SIZE, "pagina 1 contiene 0xBB");
    ASSERT_MEM(bufs[2], reads[2], PAGE_SIZE, "pagina 2 contiene 0xCC");
    return 1;
}


// ============================================================================
// BLOQUE 3: aislamiento entre páginas
// ============================================================================

static int test_page_isolation_no_bleed(void) {
    char buf0[PAGE_SIZE], buf1[PAGE_SIZE], read0[PAGE_SIZE], read1[PAGE_SIZE];
    memset(buf0, 0xAA, PAGE_SIZE);
    memset(buf1, 0xBB, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, buf0);
    write_page(TEST_DIR, "t", 1, buf1);
    load_page(TEST_DIR, "t", 0, read0);
    load_page(TEST_DIR, "t", 1, read1);
    ASSERT_MEM(buf0, read0, PAGE_SIZE, "pagina 0 no tiene datos de pagina 1");
    ASSERT_MEM(buf1, read1, PAGE_SIZE, "pagina 1 no tiene datos de pagina 0");
    return 1;
}

static int test_overwrite_does_not_affect_neighbor(void) {
    char buf0[PAGE_SIZE], buf1[PAGE_SIZE], new0[PAGE_SIZE], read1[PAGE_SIZE];
    memset(buf0, 0xAA, PAGE_SIZE);
    memset(buf1, 0xBB, PAGE_SIZE);
    memset(new0, 0xCC, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, buf0);
    write_page(TEST_DIR, "t", 1, buf1);
    // Sobrescribir página 0
    write_page(TEST_DIR, "t", 0, new0);
    // Página 1 no debe haber cambiado
    load_page(TEST_DIR, "t", 1, read1);
    ASSERT_MEM(buf1, read1, PAGE_SIZE, "overwrite pagina 0 no afecta pagina 1");
    return 1;
}

static int test_overwrite_same_page(void) {
    char bufA[PAGE_SIZE], bufB[PAGE_SIZE], read_buf[PAGE_SIZE];
    memset(bufA, 0xAA, PAGE_SIZE);
    memset(bufB, 0xBB, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, bufA);
    write_page(TEST_DIR, "t", 0, bufB);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(bufB, read_buf, PAGE_SIZE, "overwrite: segunda escritura prevalece");
    return 1;
}

static int test_read_after_overwrite_is_new_data(void) {
    char old_buf[PAGE_SIZE], new_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    memset(old_buf, 0x11, PAGE_SIZE);
    memset(new_buf, 0x22, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, old_buf);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(old_buf, read_buf, PAGE_SIZE, "primera lectura tiene datos originales");
    write_page(TEST_DIR, "t", 0, new_buf);
    memset(read_buf, 0x00, PAGE_SIZE);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(new_buf, read_buf, PAGE_SIZE, "segunda lectura tiene datos nuevos");
    return 1;
}

static int test_two_tables_no_interference(void) {
    char u[PAGE_SIZE], o[PAGE_SIZE], ru[PAGE_SIZE], ro[PAGE_SIZE];
    memset(u, 0xAA, PAGE_SIZE);
    memset(o, 0xBB, PAGE_SIZE);
    write_page(TEST_DIR, "users",  0, u);
    write_page(TEST_DIR, "orders", 0, o);
    load_page(TEST_DIR, "users",  0, ru);
    load_page(TEST_DIR, "orders", 0, ro);
    ASSERT_MEM(u, ru, PAGE_SIZE, "users no tiene datos de orders");
    ASSERT_MEM(o, ro, PAGE_SIZE, "orders no tiene datos de users");
    return 1;
}

static int test_table_filename_has_db_extension(void) {
    char buf[PAGE_SIZE];
    memset(buf, 0x42, PAGE_SIZE);
    write_page(TEST_DIR, "productos", 0, buf);
    char with_ext[256], without_ext[256];
    snprintf(with_ext,    sizeof(with_ext),    "%s/productos.db", TEST_DIR);
    snprintf(without_ext, sizeof(without_ext), "%s/productos",    TEST_DIR);
    ASSERT(access(with_ext,    F_OK) == 0, "archivo productos.db existe");
    ASSERT(access(without_ext, F_OK) != 0, "archivo productos SIN .db no existe");
    return 1;
}


// ============================================================================
// BLOQUE 4: datos binarios
// ============================================================================

static int test_binary_null_bytes_in_middle(void) {
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    for (int i = 0; i < PAGE_SIZE; i++)
        write_buf[i] = (i % 3 == 0) ? 0x00 : (char)0xFF;
    write_page(TEST_DIR, "t", 0, write_buf);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(write_buf, read_buf, PAGE_SIZE, "bytes 0x00 intercalados no truncan la pagina");
    return 1;
}

static int test_binary_all_null(void) {
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    memset(write_buf, 0x00, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, write_buf);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(write_buf, read_buf, PAGE_SIZE, "pagina de solo 0x00 se preserva completa");
    return 1;
}

static int test_binary_alternating_00_ff(void) {
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    for (int i = 0; i < PAGE_SIZE; i++)
        write_buf[i] = (i % 2 == 0) ? 0x00 : (char)0xFF;
    write_page(TEST_DIR, "t", 0, write_buf);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(write_buf, read_buf, PAGE_SIZE, "patron alternado 0x00/0xFF se preserva");
    return 1;
}

static int test_binary_all_values(void) {
    // PAGE_SIZE = 4096, cubre todos los 256 valores de byte 16 veces
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    for (int i = 0; i < PAGE_SIZE; i++)
        write_buf[i] = (char)(i % 256);
    write_page(TEST_DIR, "t", 0, write_buf);
    load_page(TEST_DIR, "t", 0, read_buf);
    ASSERT_MEM(write_buf, read_buf, PAGE_SIZE, "todos los valores de byte (0-255) se preservan");
    return 1;
}

static int test_binary_middle_page_id(void) {
    // Verifica datos binarios en página no-cero
    char write_buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    for (int i = 0; i < PAGE_SIZE; i++)
        write_buf[i] = (char)((i * 13 + 7) % 256);
    write_page(TEST_DIR, "t", 0, write_buf); // necesario para que page 5 no sea hueco vacío
    write_page(TEST_DIR, "t", 1, write_buf);
    write_page(TEST_DIR, "t", 2, write_buf);
    write_page(TEST_DIR, "t", 3, write_buf);
    write_page(TEST_DIR, "t", 4, write_buf);
    memset(write_buf, 0xDE, PAGE_SIZE);
    write_buf[0] = (char)0xAD;
    write_buf[PAGE_SIZE/2] = (char)0xBE;
    write_buf[PAGE_SIZE-1] = (char)0xEF;
    write_page(TEST_DIR, "t", 5, write_buf);
    load_page(TEST_DIR, "t", 5, read_buf);
    ASSERT_MEM(write_buf, read_buf, PAGE_SIZE, "datos binarios en pagina 5 se preservan");
    return 1;
}


// ============================================================================
// BLOQUE 5: load_page — validación de bounds (usan fork)
// ============================================================================

static int test_load_nonexistent_file_exits(void) {
    ASSERT(exits_with_failure(_helper_load_nonexistent),
           "load_page sobre archivo inexistente hace exit(1)");
    return 1;
}

static int test_load_out_of_bounds_exits(void) {
    ASSERT(exits_with_failure(_helper_load_out_of_bounds),
           "load_page con page_id >= num_pages hace exit(1)");
    return 1;
}

static int test_load_page_2_of_2_exits(void) {
    ASSERT(exits_with_failure(_helper_load_page_2_of_1),
           "load_page con page_id == num_pages hace exit(1)");
    return 1;
}

static int test_load_valid_last_page(void) {
    char buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    memset(buf, 0x99, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, buf);
    write_page(TEST_DIR, "t", 1, buf);
    write_page(TEST_DIR, "t", 2, buf);
    // Página 2 es la última válida (num_pages == 3, índices 0-2)
    load_page(TEST_DIR, "t", 2, read_buf);
    ASSERT_MEM(buf, read_buf, PAGE_SIZE, "load_page de la ultima pagina valida funciona");
    return 1;
}


// ============================================================================
// BLOQUE 6: stress y secuencias
// ============================================================================

static int test_stress_10_pages_sequential(void) {
    char write_bufs[10][PAGE_SIZE], read_buf[PAGE_SIZE];
    for (int i = 0; i < 10; i++) {
        memset(write_bufs[i], i + 1, PAGE_SIZE);
        write_page(TEST_DIR, "t", i, write_bufs[i]);
    }
    ASSERT_INT(10, get_num_pages(TEST_DIR, "t"), "stress 10 paginas: get_num_pages == 10");
    for (int i = 0; i < 10; i++) {
        load_page(TEST_DIR, "t", i, read_buf);
        ASSERT_MEM(write_bufs[i], read_buf, PAGE_SIZE, "stress: cada pagina tiene su patron");
    }
    return 1;
}

static int test_stress_100_pages(void) {
    char buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    for (int i = 0; i < 100; i++) {
        memset(buf, (i % 256), PAGE_SIZE);
        write_page(TEST_DIR, "t", i, buf);
    }
    ASSERT_INT(100, get_num_pages(TEST_DIR, "t"), "stress 100 paginas: get_num_pages == 100");
    int all_ok = 1;
    for (int i = 0; i < 100; i++) {
        load_page(TEST_DIR, "t", i, read_buf);
        for (int j = 0; j < PAGE_SIZE; j++) {
            if ((unsigned char)read_buf[j] != (unsigned char)(i % 256)) {
                all_ok = 0; break;
            }
        }
        if (!all_ok) break;
    }
    ASSERT(all_ok, "stress 100 paginas: todas las paginas tienen el patron correcto");
    return 1;
}

static int test_stress_alternating_write_read(void) {
    char buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    for (int i = 0; i < 20; i++) {
        memset(buf, i + 1, PAGE_SIZE);
        write_page(TEST_DIR, "t", i, buf);
        load_page(TEST_DIR, "t", i, read_buf);
        ASSERT_MEM(buf, read_buf, PAGE_SIZE, "write->read inmediato: datos correctos");
    }
    return 1;
}

static int test_stress_same_page_100_overwrites(void) {
    char buf[PAGE_SIZE], read_buf[PAGE_SIZE];
    for (int i = 0; i < 100; i++) {
        memset(buf, i % 256, PAGE_SIZE);
        write_page(TEST_DIR, "t", 0, buf);
    }
    // Solo debe quedar la última escritura
    load_page(TEST_DIR, "t", 0, read_buf);
    memset(buf, 99, PAGE_SIZE); // 99 = 99 % 256
    ASSERT_MEM(buf, read_buf, PAGE_SIZE, "100 overwrites en pagina 0: queda la ultima");
    ASSERT_INT(1, get_num_pages(TEST_DIR, "t"), "100 overwrites no crean paginas extra");
    return 1;
}

static int test_stress_interleaved_tables(void) {
    char buf_u[PAGE_SIZE], buf_o[PAGE_SIZE];
    char read_u[PAGE_SIZE], read_o[PAGE_SIZE];

    // Intercalar escrituras en dos tablas
    for (int i = 0; i < 5; i++) {
        memset(buf_u, 0xAA + i, PAGE_SIZE);
        memset(buf_o, 0x55 + i, PAGE_SIZE);
        write_page(TEST_DIR, "users",  i, buf_u);
        write_page(TEST_DIR, "orders", i, buf_o);
    }

    // Verificar que no hay contaminación cruzada
    int all_ok = 1;
    for (int i = 0; i < 5; i++) {
        memset(buf_u, 0xAA + i, PAGE_SIZE);
        memset(buf_o, 0x55 + i, PAGE_SIZE);
        load_page(TEST_DIR, "users",  i, read_u);
        load_page(TEST_DIR, "orders", i, read_o);
        if (memcmp(buf_u, read_u, PAGE_SIZE) != 0) { all_ok = 0; break; }
        if (memcmp(buf_o, read_o, PAGE_SIZE) != 0) { all_ok = 0; break; }
    }
    ASSERT(all_ok, "escrituras intercaladas en 2 tablas: sin contaminacion cruzada");
    return 1;
}


// ============================================================================
// BLOQUE 7: casos límite y propiedades de PAGE_SIZE
// ============================================================================

static int test_page_size_is_4096(void) {
    ASSERT_INT(4096, PAGE_SIZE, "PAGE_SIZE == 4096");
    return 1;
}

static int test_written_file_size_is_multiple_of_page_size(void) {
    char buf[PAGE_SIZE];
    memset(buf, 0x42, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, buf);
    write_page(TEST_DIR, "t", 1, buf);
    write_page(TEST_DIR, "t", 2, buf);

    char path[256];
    snprintf(path, sizeof(path), "%s/t.db", TEST_DIR);
    FILE *f = fopen(path, "rb");
    ASSERT(f != NULL, "archivo .db existe y se puede abrir");
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fclose(f);

    ASSERT(size == 3 * PAGE_SIZE, "tamanio del archivo == 3 * PAGE_SIZE");
    ASSERT(size % PAGE_SIZE == 0, "tamanio del archivo es multiplo de PAGE_SIZE");
    return 1;
}

static int test_load_preserves_buffer_size(void) {
    // Asegura que load_page no escribe más ni menos de PAGE_SIZE bytes en el buffer
    // Ponemos centinelas antes y después del buffer para detectar desbordamientos
    char guarded[PAGE_SIZE + 16];
    memset(guarded, 0xCC, sizeof(guarded));
    char write_buf[PAGE_SIZE];
    memset(write_buf, 0xAB, PAGE_SIZE);
    write_page(TEST_DIR, "t", 0, write_buf);

    load_page(TEST_DIR, "t", 0, guarded + 8);

    // Centinelas antes del buffer no deben haber sido tocados
    for (int i = 0; i < 8; i++)
        ASSERT((unsigned char)guarded[i] == 0xCC, "centinela antes del buffer no fue tocado");
    // Centinelas después del buffer no deben haber sido tocados
    for (int i = 0; i < 8; i++)
        ASSERT((unsigned char)guarded[PAGE_SIZE + 8 + i] == 0xCC, "centinela despues del buffer no fue tocado");

    return 1;
}


// ============================================================================
// MAIN
// ============================================================================

int main(void) {
    // Crear directorio temporal único
    strcpy(TEST_DIR, "/tmp/test_disk_XXXXXX");
    if (!mkdtemp(TEST_DIR)) {
        perror("mkdtemp");
        return 1;
    }
    strcpy(TEST_DIR_GLOBAL, TEST_DIR);

    printf("=== TEST DISK ===\n");
    printf("Directorio temporal: %s\n\n", TEST_DIR);

    printf("-- Bloque 1: get_num_pages --\n");
    RUN_TEST(test_num_pages_no_file);
    RUN_TEST(test_num_pages_after_one_write);
    RUN_TEST(test_num_pages_after_three_writes);
    RUN_TEST(test_num_pages_with_gap);
    RUN_TEST(test_num_pages_same_table_twice);
    RUN_TEST(test_num_pages_two_tables_independent);
    RUN_TEST(test_num_pages_overwrite_doesnt_grow);

    printf("\n-- Bloque 2: write_page + load_page roundtrip --\n");
    RUN_TEST(test_roundtrip_zeros);
    RUN_TEST(test_roundtrip_ff);
    RUN_TEST(test_roundtrip_pattern_ab);
    RUN_TEST(test_roundtrip_cyclic_pattern);
    RUN_TEST(test_roundtrip_full_4096_bytes);
    RUN_TEST(test_roundtrip_first_and_last_byte);
    RUN_TEST(test_write_creates_file);
    RUN_TEST(test_roundtrip_multiple_pages);

    printf("\n-- Bloque 3: aislamiento entre paginas --\n");
    RUN_TEST(test_page_isolation_no_bleed);
    RUN_TEST(test_overwrite_does_not_affect_neighbor);
    RUN_TEST(test_overwrite_same_page);
    RUN_TEST(test_read_after_overwrite_is_new_data);
    RUN_TEST(test_two_tables_no_interference);
    RUN_TEST(test_table_filename_has_db_extension);

    printf("\n-- Bloque 4: datos binarios --\n");
    RUN_TEST(test_binary_null_bytes_in_middle);
    RUN_TEST(test_binary_all_null);
    RUN_TEST(test_binary_alternating_00_ff);
    RUN_TEST(test_binary_all_values);
    RUN_TEST(test_binary_middle_page_id);

    printf("\n-- Bloque 5: load_page bounds (fork) --\n");
    RUN_TEST(test_load_nonexistent_file_exits);
    RUN_TEST(test_load_out_of_bounds_exits);
    RUN_TEST(test_load_page_2_of_2_exits);
    RUN_TEST(test_load_valid_last_page);

    printf("\n-- Bloque 6: stress --\n");
    RUN_TEST(test_stress_10_pages_sequential);
    RUN_TEST(test_stress_100_pages);
    RUN_TEST(test_stress_alternating_write_read);
    RUN_TEST(test_stress_same_page_100_overwrites);
    RUN_TEST(test_stress_interleaved_tables);

    printf("\n-- Bloque 7: limites y propiedades --\n");
    RUN_TEST(test_page_size_is_4096);
    RUN_TEST(test_written_file_size_is_multiple_of_page_size);
    RUN_TEST(test_load_preserves_buffer_size);

    // Limpiar
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DIR);
    system(cmd);

    printf("\n=== RESULTADO: %d passed, %d failed, %d total ===\n",
           tests_passed, tests_failed, tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}