// test_page.c — Test exhaustivo del módulo page
// Compilar: gcc -Wall -Wextra -g -o test_page test_page.c ../page.c ../disk.c
// Ejecutar: ./test_page

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../page.h"

// ============================================================================
// Infraestructura
// ============================================================================

static int tests_passed = 0;
static int tests_failed  = 0;

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

#define ASSERT_PTR_NULL(ptr, msg) do { \
    if ((ptr) != NULL) { \
        printf("    FAIL: %s — esperado NULL, obtenido puntero no-NULL (linea %d)\n", \
               msg, __LINE__); \
        tests_failed++; \
        return 0; \
    } \
} while(0)

#define ASSERT_PTR_NOTNULL(ptr, msg) do { \
    if ((ptr) == NULL) { \
        printf("    FAIL: %s — esperado puntero no-NULL, obtenido NULL (linea %d)\n", \
               msg, __LINE__); \
        tests_failed++; \
        return 0; \
    } \
} while(0)

#define RUN_TEST(fn) do { \
    printf("  %-65s", #fn); \
    fflush(stdout); \
    int _r = fn(); \
    if (_r) { printf("PASS\n"); tests_passed++; } \
    else    { printf("(ver arriba)\n"); } \
} while(0)


// ============================================================================
// BLOQUE 1: init_page
// ============================================================================

static int test_init_page_header_values(void) {
    char page[PAGE_SIZE];
    init_page(page);
    PageHeader *h = (PageHeader *)page;
    ASSERT_INT(0,           h->num_slots,        "num_slots == 0 tras init");
    ASSERT_INT((int)sizeof(PageHeader), h->free_space_start, "free_space_start == sizeof(PageHeader)");
    ASSERT_INT(PAGE_SIZE,   h->free_space_end,   "free_space_end == PAGE_SIZE");
    return 1;
}

static int test_init_page_free_space(void) {
    char page[PAGE_SIZE];
    init_page(page);
    PageHeader *h = (PageHeader *)page;
    int expected = PAGE_SIZE - (int)sizeof(PageHeader);
    ASSERT_INT(expected, h->free_space_end - h->free_space_start,
               "espacio libre inicial == PAGE_SIZE - sizeof(PageHeader)");
    return 1;
}

static int test_init_page_twice_resets(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char data[] = "hola";
    insert_row(page, data, 4);
    // Reinicializar debe resetear todo
    init_page(page);
    PageHeader *h = (PageHeader *)page;
    ASSERT_INT(0, h->num_slots, "num_slots == 0 tras segundo init_page");
    ASSERT_INT(PAGE_SIZE, h->free_space_end, "free_space_end == PAGE_SIZE tras segundo init");
    return 1;
}


// ============================================================================
// BLOQUE 2: insert_row + read_row básico
// ============================================================================

static int test_insert_single_row(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char data[] = "hello";
    int slot = insert_row(page, data, 5);
    ASSERT_INT(0, slot, "primer insert retorna slot 0");
    char *r = read_row(page, slot);
    ASSERT_PTR_NOTNULL(r, "read_row retorna no-NULL");
    ASSERT_MEM(data, r, 5, "contenido leido coincide con lo insertado");
    ASSERT_INT(5, get_row_size(page, slot), "get_row_size retorna 5");
    return 1;
}

static int test_insert_slots_sequential(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char d1[] = "one", d2[] = "two", d3[] = "three";
    int s1 = insert_row(page, d1, 3);
    int s2 = insert_row(page, d2, 3);
    int s3 = insert_row(page, d3, 5);
    ASSERT_INT(0, s1, "primer slot es 0");
    ASSERT_INT(1, s2, "segundo slot es 1");
    ASSERT_INT(2, s3, "tercer slot es 2");
    return 1;
}

static int test_insert_multiple_data_correct(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char *data[] = {"alpha", "beta", "gamma", "delta", "epsilon"};
    int   sizes[] = {5, 4, 5, 5, 7};
    int slots[5];
    for (int i = 0; i < 5; i++)
        slots[i] = insert_row(page, data[i], sizes[i]);
    for (int i = 0; i < 5; i++) {
        char *r = read_row(page, slots[i]);
        ASSERT_PTR_NOTNULL(r, "read_row no-NULL para cada fila");
        ASSERT_MEM(data[i], r, sizes[i], "contenido correcto para cada fila");
        ASSERT_INT(sizes[i], get_row_size(page, slots[i]), "get_row_size correcto");
    }
    return 1;
}

static int test_insert_updates_num_slots(void) {
    char page[PAGE_SIZE];
    init_page(page);
    PageHeader *h = (PageHeader *)page;
    char d[] = "x";
    insert_row(page, d, 1);
    ASSERT_INT(1, h->num_slots, "num_slots == 1 tras primer insert");
    insert_row(page, d, 1);
    ASSERT_INT(2, h->num_slots, "num_slots == 2 tras segundo insert");
    insert_row(page, d, 1);
    ASSERT_INT(3, h->num_slots, "num_slots == 3 tras tercer insert");
    return 1;
}

static int test_insert_1_byte(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char b = 'Z';
    int slot = insert_row(page, &b, 1);
    ASSERT(slot >= 0, "insert de 1 byte retorna slot valido");
    ASSERT_INT(1, get_row_size(page, slot), "get_row_size == 1");
    ASSERT(*read_row(page, slot) == 'Z', "byte leido es correcto");
    return 1;
}

static int test_insert_empty_row(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char empty = 0;
    int slot = insert_row(page, &empty, 0);
    ASSERT(slot >= 0, "insert de 0 bytes retorna slot valido");
    ASSERT_PTR_NOTNULL(read_row(page, slot), "read_row de fila vacia no es NULL");
    ASSERT_INT(0, get_row_size(page, slot), "get_row_size == 0 para fila vacia");
    return 1;
}


// ============================================================================
// BLOQUE 3: delete_row
// ============================================================================

static int test_delete_marks_null(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char d[] = "borrar";
    int slot = insert_row(page, d, 6);
    delete_row(page, slot);
    ASSERT_PTR_NULL(read_row(page, slot), "read_row retorna NULL tras delete");
    ASSERT_INT(-1, get_row_size(page, slot), "get_row_size retorna -1 tras delete");
    return 1;
}

static int test_delete_middle_slot(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char d1[] = "uno", d2[] = "dos", d3[] = "tres";
    int s1 = insert_row(page, d1, 3);
    int s2 = insert_row(page, d2, 3);
    int s3 = insert_row(page, d3, 4);
    delete_row(page, s2);
    ASSERT_PTR_NULL(read_row(page, s2), "slot 1 borrado retorna NULL");
    // Slots vecinos intactos
    ASSERT_MEM(d1, read_row(page, s1), 3, "slot 0 intacto tras borrar slot 1");
    ASSERT_MEM(d3, read_row(page, s3), 4, "slot 2 intacto tras borrar slot 1");
    return 1;
}

static int test_delete_invalid_slots_no_crash(void) {
    char page[PAGE_SIZE];
    init_page(page);
    // Ninguna de estas llamadas debe crashear
    delete_row(page, -1);
    delete_row(page, 0);
    delete_row(page, 100);
    delete_row(page, 999);
    // La página sigue funcional
    char d[] = "ok";
    int slot = insert_row(page, d, 2);
    ASSERT_INT(0, slot, "pagina sigue funcional tras deletes invalidos");
    return 1;
}

static int test_delete_all_slots(void) {
    char page[PAGE_SIZE];
    init_page(page);
    int slots[5];
    char d[10];
    for (int i = 0; i < 5; i++) {
        sprintf(d, "fila%d", i);
        slots[i] = insert_row(page, d, strlen(d));
    }
    for (int i = 0; i < 5; i++)
        delete_row(page, slots[i]);
    for (int i = 0; i < 5; i++)
        ASSERT_PTR_NULL(read_row(page, slots[i]), "todos los slots retornan NULL tras borrar");
    return 1;
}


// ============================================================================
// BLOQUE 4: reutilización de slots
// ============================================================================

static int test_reuse_deleted_slot(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char d1[] = "primero", d2[] = "segundo", d3[] = "tercero";
    int s1 = insert_row(page, d1, 7);
    int s2 = insert_row(page, d2, 7);
    int s3 = insert_row(page, d3, 7);
    (void)s1; (void)s3;
    delete_row(page, s2);
    char d4[] = "nuevo";
    int s4 = insert_row(page, d4, 5);
    ASSERT_INT(1, s4, "insertar tras borrar slot 1 reutiliza slot 1");
    ASSERT_MEM(d4, read_row(page, s4), 5, "dato en slot reutilizado es correcto");
    return 1;
}

static int test_reuse_first_deleted(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char d[] = "x";
    int s0 = insert_row(page, d, 1);
    int s1 = insert_row(page, d, 1);
    int s2 = insert_row(page, d, 1);
    (void)s1; (void)s2;
    delete_row(page, s0);
    char d2[] = "reuso";
    int sn = insert_row(page, d2, 5);
    // La implementación busca el primer slot borrado → debe ser 0
    ASSERT_INT(0, sn, "primer slot borrado (slot 0) es reutilizado");
    ASSERT_MEM(d2, read_row(page, sn), 5, "dato correcto en slot reutilizado");
    return 1;
}

static int test_num_slots_does_not_grow_on_reuse(void) {
    char page[PAGE_SIZE];
    init_page(page);
    PageHeader *h = (PageHeader *)page;
    char d[] = "dato";
    insert_row(page, d, 4);
    insert_row(page, d, 4);
    insert_row(page, d, 4);
    ASSERT_INT(3, h->num_slots, "num_slots == 3 tras 3 inserts");
    delete_row(page, 1);
    insert_row(page, d, 4); // reutiliza slot 1
    ASSERT_INT(3, h->num_slots, "num_slots sigue siendo 3 tras reuso de slot");
    return 1;
}

static int test_reuse_all_deleted_slots(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char d[10];
    int slots[5];
    for (int i = 0; i < 5; i++) {
        sprintf(d, "r%d", i);
        slots[i] = insert_row(page, d, strlen(d));
    }
    for (int i = 0; i < 5; i++)
        delete_row(page, slots[i]);
    // Reinsertar 5 filas nuevas — deben reutilizar todos los slots
    for (int i = 0; i < 5; i++) {
        sprintf(d, "n%d", i);
        int sn = insert_row(page, d, strlen(d));
        ASSERT(sn >= 0 && sn < 5, "slot reutilizado esta dentro del rango 0-4");
        ASSERT_MEM(d, read_row(page, sn), strlen(d), "dato correcto en slot reutilizado");
    }
    return 1;
}


// ============================================================================
// BLOQUE 5: slots inválidos
// ============================================================================

static int test_read_row_invalid_negative(void) {
    char page[PAGE_SIZE];
    init_page(page);
    ASSERT_PTR_NULL(read_row(page, -1), "read_row(-1) retorna NULL");
    return 1;
}

static int test_read_row_invalid_out_of_range(void) {
    char page[PAGE_SIZE];
    init_page(page);
    ASSERT_PTR_NULL(read_row(page, 0),   "read_row(0) en pagina vacia retorna NULL");
    ASSERT_PTR_NULL(read_row(page, 100), "read_row(100) en pagina vacia retorna NULL");
    ASSERT_PTR_NULL(read_row(page, 999), "read_row(999) en pagina vacia retorna NULL");
    return 1;
}

static int test_get_row_size_invalid(void) {
    char page[PAGE_SIZE];
    init_page(page);
    ASSERT_INT(-1, get_row_size(page, -1),  "get_row_size(-1) retorna -1");
    ASSERT_INT(-1, get_row_size(page, 0),   "get_row_size(0) en pagina vacia retorna -1");
    ASSERT_INT(-1, get_row_size(page, 100), "get_row_size(100) retorna -1");
    return 1;
}


// ============================================================================
// BLOQUE 6: capacidad y espacio
// ============================================================================

static int test_insert_returns_minus1_when_full(void) {
    char page[PAGE_SIZE];
    init_page(page);
    // Llenar con filas pequeñas hasta que no cabe más
    char d[9] = "XXXXXXXX";
    int last_valid = -1, slot;
    while ((slot = insert_row(page, d, 8)) >= 0)
        last_valid = slot;
    ASSERT_INT(-1, slot, "insert_row retorna -1 cuando la pagina esta llena");
    // El ultimo slot valido sigue siendo legible
    ASSERT(last_valid >= 0, "hubo al menos un insert exitoso");
    ASSERT_PTR_NOTNULL(read_row(page, last_valid), "ultimo slot valido sigue legible");
    return 1;
}

static int test_page_still_works_after_failed_insert(void) {
    char page[PAGE_SIZE];
    init_page(page);
    // Intentar insertar algo demasiado grande
    char *huge = malloc(PAGE_SIZE);
    memset(huge, 'X', PAGE_SIZE);
    int slot = insert_row(page, huge, PAGE_SIZE);
    free(huge);
    ASSERT_INT(-1, slot, "insert de PAGE_SIZE bytes retorna -1");
    // La pagina sigue funcional
    char d[] = "ok";
    slot = insert_row(page, d, 2);
    ASSERT_INT(0, slot, "pagina funcional tras insert fallido");
    ASSERT_MEM(d, read_row(page, 0), 2, "dato correcto tras insert fallido");
    return 1;
}

static int test_header_tracks_free_space(void) {
    char page[PAGE_SIZE];
    init_page(page);
    PageHeader *h = (PageHeader *)page;
    int start_before = h->free_space_start;
    int end_before   = h->free_space_end;
    char d[] = "test"; // 4 bytes
    insert_row(page, d, 4);
    // free_space_start debe crecer en sizeof(SlotEntry) (entrada de slot)
    ASSERT_INT(start_before + (int)sizeof(SlotEntry), h->free_space_start,
               "free_space_start crece en sizeof(SlotEntry) tras insert");
    // free_space_end debe decrecer en sizeof(int)+4 (size header + datos)
    ASSERT_INT(end_before - (int)(sizeof(int) + 4), h->free_space_end,
               "free_space_end decrece en sizeof(int)+4 tras insert");
    return 1;
}

static int test_free_space_consistent_after_multiple_inserts(void) {
    char page[PAGE_SIZE];
    init_page(page);
    PageHeader *h = (PageHeader *)page;
    char d[] = "hola"; // 4 bytes
    for (int i = 0; i < 10; i++)
        insert_row(page, d, 4);
    // Verificar que free_space_start < free_space_end (invariante)
    ASSERT(h->free_space_start < h->free_space_end,
           "free_space_start < free_space_end tras 10 inserts");
    ASSERT(h->free_space_start >= (int)sizeof(PageHeader),
           "free_space_start >= sizeof(PageHeader)");
    ASSERT(h->free_space_end <= PAGE_SIZE,
           "free_space_end <= PAGE_SIZE");
    return 1;
}


// ============================================================================
// BLOQUE 7: datos binarios
// ============================================================================

static int test_binary_all_byte_values(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char data[256];
    for (int i = 0; i < 256; i++)
        data[i] = (char)i;
    int slot = insert_row(page, data, 256);
    ASSERT(slot >= 0, "insert de 256 bytes (todos los valores) retorna slot valido");
    ASSERT_INT(256, get_row_size(page, slot), "get_row_size == 256");
    ASSERT_MEM(data, read_row(page, slot), 256, "todos los bytes leidos son correctos");
    return 1;
}

static int test_binary_null_bytes_in_middle(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char data[5] = {0x01, 0x00, 0x02, 0x00, 0x03};
    int slot = insert_row(page, data, 5);
    ASSERT(slot >= 0, "insert con bytes 0x00 en el medio retorna slot valido");
    // get_row_size debe retornar 5, no 1 (no debe tratar 0x00 como terminador)
    ASSERT_INT(5, get_row_size(page, slot),
               "get_row_size retorna 5 (0x00 no es terminador)");
    ASSERT_MEM(data, read_row(page, slot), 5, "5 bytes leidos correctamente");
    return 1;
}

static int test_binary_all_zeros(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char data[100];
    memset(data, 0x00, 100);
    int slot = insert_row(page, data, 100);
    ASSERT(slot >= 0, "insert de 100 bytes a 0x00 retorna slot valido");
    ASSERT_INT(100, get_row_size(page, slot), "get_row_size == 100");
    ASSERT_MEM(data, read_row(page, slot), 100, "100 bytes a 0x00 leidos correctamente");
    return 1;
}

static int test_binary_all_ff(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char data[100];
    memset(data, 0xFF, 100);
    int slot = insert_row(page, data, 100);
    ASSERT(slot >= 0, "insert de 100 bytes a 0xFF retorna slot valido");
    ASSERT_MEM(data, read_row(page, slot), 100, "100 bytes a 0xFF leidos correctamente");
    return 1;
}

static int test_binary_first_and_last_byte(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char data[50];
    memset(data, 0x00, 50);
    data[0]  = (char)0xDE;
    data[49] = (char)0xAD;
    int slot = insert_row(page, data, 50);
    char *r = read_row(page, slot);
    ASSERT((unsigned char)r[0]  == 0xDE, "primer byte es 0xDE");
    ASSERT((unsigned char)r[49] == 0xAD, "ultimo byte es 0xAD");
    return 1;
}


// ============================================================================
// BLOQUE 8: stress
// ============================================================================

static int test_stress_fill_with_small_rows(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char d[5] = "abcd"; // 4 bytes
    int count = 0, slot;
    while ((slot = insert_row(page, d, 4)) >= 0)
        count++;
    ASSERT(count >= 50, "caben al menos 50 filas de 4 bytes en una pagina");
    // Verificar que el ultimo slot insertado sigue siendo correcto
    ASSERT_PTR_NOTNULL(read_row(page, slot - 1 < 0 ? 0 : count - 1),
                       "ultimo slot insertado sigue legible");
    return 1;
}

static int test_stress_delete_even_reinsert(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char d[10];
    int slots[10];
    for (int i = 0; i < 10; i++) {
        sprintf(d, "dato%d", i);
        slots[i] = insert_row(page, d, strlen(d));
    }
    // Borrar slots pares
    for (int i = 0; i < 10; i += 2)
        delete_row(page, slots[i]);
    // Reinsertar en slots pares
    for (int i = 0; i < 10; i += 2) {
        sprintf(d, "new%d", i);
        int sn = insert_row(page, d, strlen(d));
        ASSERT_INT(slots[i], sn, "reinsert en slot par reutiliza slot correcto");
    }
    // Slots impares intactos
    for (int i = 1; i < 10; i += 2) {
        sprintf(d, "dato%d", i);
        char *r = read_row(page, slots[i]);
        ASSERT_PTR_NOTNULL(r, "slot impar sigue no-NULL");
        ASSERT_MEM(d, r, strlen(d), "slot impar tiene dato original");
    }
    return 1;
}

static int test_stress_alternating_insert_delete(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char d[10];
    // 50 ciclos insert → delete → insert
    for (int i = 0; i < 50; i++) {
        sprintf(d, "i%d", i);
        int slot = insert_row(page, d, strlen(d));
        ASSERT(slot >= 0, "insert en ciclo alternado retorna slot valido");
        delete_row(page, slot);
    }
    // Insertar fila final y verificar
    char final[] = "final";
    int slot = insert_row(page, final, 5);
    ASSERT(slot >= 0, "insert final tras 50 ciclos retorna slot valido");
    ASSERT_MEM(final, read_row(page, slot), 5, "dato final correcto");
    return 1;
}

static int test_stress_100_inserts_verify_all(void) {
    char page[PAGE_SIZE];
    init_page(page);
    char data[8];
    int slots[200]; // puede que quepan menos de 100, reservamos más
    int count = 0;
    for (int i = 0; i < 200 && count < 200; i++) {
        sprintf(data, "r%03d", i);
        int slot = insert_row(page, data, strlen(data));
        if (slot < 0) break;
        slots[count++] = slot;
    }
    ASSERT(count >= 50, "caben al menos 50 filas de 4 bytes");
    // Verificar todas las filas insertadas
    int all_ok = 1;
    for (int i = 0; i < count; i++) {
        sprintf(data, "r%03d", i);
        char *r = read_row(page, slots[i]);
        if (r == NULL || memcmp(r, data, strlen(data)) != 0) {
            all_ok = 0; break;
        }
        if (get_row_size(page, slots[i]) != (int)strlen(data)) {
            all_ok = 0; break;
        }
    }
    ASSERT(all_ok, "todas las filas insertadas tienen el dato correcto");
    return 1;
}


// ============================================================================
// MAIN
// ============================================================================

int main(void) {
    printf("=== TEST PAGE ===\n\n");

    printf("-- Bloque 1: init_page --\n");
    RUN_TEST(test_init_page_header_values);
    RUN_TEST(test_init_page_free_space);
    RUN_TEST(test_init_page_twice_resets);

    printf("\n-- Bloque 2: insert_row + read_row basico --\n");
    RUN_TEST(test_insert_single_row);
    RUN_TEST(test_insert_slots_sequential);
    RUN_TEST(test_insert_multiple_data_correct);
    RUN_TEST(test_insert_updates_num_slots);
    RUN_TEST(test_insert_1_byte);
    RUN_TEST(test_insert_empty_row);

    printf("\n-- Bloque 3: delete_row --\n");
    RUN_TEST(test_delete_marks_null);
    RUN_TEST(test_delete_middle_slot);
    RUN_TEST(test_delete_invalid_slots_no_crash);
    RUN_TEST(test_delete_all_slots);

    printf("\n-- Bloque 4: reutilizacion de slots --\n");
    RUN_TEST(test_reuse_deleted_slot);
    RUN_TEST(test_reuse_first_deleted);
    RUN_TEST(test_num_slots_does_not_grow_on_reuse);
    RUN_TEST(test_reuse_all_deleted_slots);

    printf("\n-- Bloque 5: slots invalidos --\n");
    RUN_TEST(test_read_row_invalid_negative);
    RUN_TEST(test_read_row_invalid_out_of_range);
    RUN_TEST(test_get_row_size_invalid);

    printf("\n-- Bloque 6: capacidad y espacio --\n");
    RUN_TEST(test_insert_returns_minus1_when_full);
    RUN_TEST(test_page_still_works_after_failed_insert);
    RUN_TEST(test_header_tracks_free_space);
    RUN_TEST(test_free_space_consistent_after_multiple_inserts);

    printf("\n-- Bloque 7: datos binarios --\n");
    RUN_TEST(test_binary_all_byte_values);
    RUN_TEST(test_binary_null_bytes_in_middle);
    RUN_TEST(test_binary_all_zeros);
    RUN_TEST(test_binary_all_ff);
    RUN_TEST(test_binary_first_and_last_byte);

    printf("\n-- Bloque 8: stress --\n");
    RUN_TEST(test_stress_fill_with_small_rows);
    RUN_TEST(test_stress_delete_even_reinsert);
    RUN_TEST(test_stress_alternating_insert_delete);
    RUN_TEST(test_stress_100_inserts_verify_all);

    printf("\n=== RESULTADO: %d passed, %d failed, %d total ===\n",
           tests_passed, tests_failed, tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}