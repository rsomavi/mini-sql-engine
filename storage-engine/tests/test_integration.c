// test_integration.c — Test de integración schema + heap
// Verifica que los cuatro módulos funcionan correctamente juntos:
// schema_save (page 0) + insert_into_table (pages 1+) + recover + row_deserialize
//
// Compilar: gcc -Wall -Wextra -g -o test_integration test_integration.c \
//               ../schema.c ../heap.c ../page.c ../disk.c
// Ejecutar: ./test_integration

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../schema.h"
#include "../heap.h"

// ============================================================================
// Infraestructura
// ============================================================================

static int tests_passed = 0;
static int tests_failed  = 0;
static char TEST_DIR[64];

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

#define ASSERT_STR(expected, actual, msg) do { \
    if (strcmp(expected, actual) != 0) { \
        printf("    FAIL: %s — esperado '%s', obtenido '%s' (linea %d)\n", \
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

// ============================================================================
// Helpers
// ============================================================================

// Construye el schema de users: id INT PK, name VARCHAR(50), age INT, city VARCHAR(30)
static Schema make_users_schema(void) {
    Schema s;
    memset(&s, 0, sizeof(Schema));
    strcpy(s.table_name, "users");
    s.num_columns = 4;

    strcpy(s.columns[0].name, "id");
    s.columns[0].type = TYPE_INT; s.columns[0].max_size = 4;
    s.columns[0].nullable = 0;   s.columns[0].is_primary_key = 1;

    strcpy(s.columns[1].name, "name");
    s.columns[1].type = TYPE_VARCHAR; s.columns[1].max_size = 50;
    s.columns[1].nullable = 0;        s.columns[1].is_primary_key = 0;

    strcpy(s.columns[2].name, "age");
    s.columns[2].type = TYPE_INT; s.columns[2].max_size = 4;
    s.columns[2].nullable = 0;   s.columns[2].is_primary_key = 0;

    strcpy(s.columns[3].name, "city");
    s.columns[3].type = TYPE_VARCHAR; s.columns[3].max_size = 30;
    s.columns[3].nullable = 0;        s.columns[3].is_primary_key = 0;

    return s;
}

// Construye el schema de products: id INT PK, name VARCHAR(50), price INT
static Schema make_products_schema(void) {
    Schema s;
    memset(&s, 0, sizeof(Schema));
    strcpy(s.table_name, "products");
    s.num_columns = 3;

    strcpy(s.columns[0].name, "id");
    s.columns[0].type = TYPE_INT; s.columns[0].max_size = 4;
    s.columns[0].nullable = 0;   s.columns[0].is_primary_key = 1;

    strcpy(s.columns[1].name, "name");
    s.columns[1].type = TYPE_VARCHAR; s.columns[1].max_size = 50;
    s.columns[1].nullable = 0;        s.columns[1].is_primary_key = 0;

    strcpy(s.columns[2].name, "price");
    s.columns[2].type = TYPE_INT; s.columns[2].max_size = 4;
    s.columns[2].nullable = 0;   s.columns[2].is_primary_key = 0;

    return s;
}

// Serializa una fila y la inserta en la tabla. Retorna el rowid.
static int insert_user(const char *dir, const Schema *s,
                        int id, const char *name, int age, const char *city) {
    void *values[] = {&id, (void*)name, &age, (void*)city};
    int   sizes[]  = {4, (int)strlen(name), 4, (int)strlen(city)};
    char  buf[PAGE_SIZE];
    int   out_size;
    if (row_serialize(s, values, sizes, buf, PAGE_SIZE, &out_size) != 0)
        return -1;
    return insert_into_table(dir, s->table_name, buf, out_size);
}

static int insert_product(const char *dir, const Schema *s,
                           int id, const char *name, int price) {
    void *values[] = {&id, (void*)name, &price};
    int   sizes[]  = {4, (int)strlen(name), 4};
    char  buf[PAGE_SIZE];
    int   out_size;
    if (row_serialize(s, values, sizes, buf, PAGE_SIZE, &out_size) != 0)
        return -1;
    return insert_into_table(dir, s->table_name, buf, out_size);
}

// Recupera una fila por rowid y la deserializa en los campos dados.
// Retorna 1 si ok, 0 si fallo.
static int recover_user(const char *dir, const Schema *s, int rowid,
                         int *out_id, char *out_name, int *out_age, char *out_city) {
    int page_id = decode_rowid_page(rowid);
    int slot_id = decode_rowid_slot(rowid);
    char page[PAGE_SIZE];
    load_page(dir, s->table_name, page_id, page);
    char *row = read_row(page, slot_id);
    if (!row) return 0;
    int row_size = get_row_size(page, slot_id);
    void  *vo[] = {out_id, out_name, out_age, out_city};
    int    so[4];
    int r = row_deserialize(s, row, row_size, vo, so);
    if (r < 0) return 0;
    // null-terminar los varchars
    out_name[so[1]] = '\0';
    out_city[so[3]] = '\0';
    return 1;
}

static int recover_product(const char *dir, const Schema *s, int rowid,
                            int *out_id, char *out_name, int *out_price) {
    int page_id = decode_rowid_page(rowid);
    int slot_id = decode_rowid_slot(rowid);
    char page[PAGE_SIZE];
    load_page(dir, s->table_name, page_id, page);
    char *row = read_row(page, slot_id);
    if (!row) return 0;
    int row_size = get_row_size(page, slot_id);
    void  *vo[] = {out_id, out_name, out_price};
    int    so[3];
    int r = row_deserialize(s, row, row_size, vo, so);
    if (r < 0) return 0;
    out_name[so[1]] = '\0';
    return 1;
}


// ============================================================================
// BLOQUE 1: schema en page 0, datos en page 1+
// ============================================================================

static int test_schema_page0_data_page1(void) {
    Schema s = make_users_schema();
    // Guardar schema en page 0
    ASSERT_INT(0, schema_save(&s, TEST_DIR), "schema_save retorna 0");
    ASSERT(get_num_pages(TEST_DIR, "users") >= 1, "page 0 existe tras schema_save");

    // Insertar una fila — debe ir a page 1
    int rowid = insert_user(TEST_DIR, &s, 1, "Juan", 25, "Madrid");
    ASSERT(rowid > 0, "insert_user retorna rowid valido");
    ASSERT_INT(1, decode_rowid_page(rowid), "primera fila va a page 1");

    // Page 0 sigue siendo el schema
    Schema loaded; memset(&loaded, 0, sizeof(Schema));
    ASSERT_INT(0, schema_load(&loaded, "users", TEST_DIR), "schema_load tras insert retorna 0");
    ASSERT_STR("users", loaded.table_name, "schema en page 0 intacto tras insert");
    ASSERT_INT(4, loaded.num_columns, "num_columns intacto tras insert");
    return 1;
}

static int test_schema_not_overwritten_by_inserts(void) {
    Schema s = make_users_schema();
    schema_save(&s, TEST_DIR);

    // Insertar muchas filas para forzar varias páginas
    for (int i = 0; i < 50; i++)
        insert_user(TEST_DIR, &s, i, "TestUser", 20, "City");

    // Schema sigue correcto
    Schema loaded; memset(&loaded, 0, sizeof(Schema));
    ASSERT_INT(0, schema_load(&loaded, "users", TEST_DIR), "schema_load tras 50 inserts");
    ASSERT_STR("users", loaded.table_name, "table_name intacto tras 50 inserts");
    ASSERT_INT(4, loaded.num_columns, "num_columns intacto tras 50 inserts");
    ASSERT_STR("id",   loaded.columns[0].name, "col 0 intacta tras 50 inserts");
    ASSERT_STR("name", loaded.columns[1].name, "col 1 intacta tras 50 inserts");
    return 1;
}


// ============================================================================
// BLOQUE 2: insertar y recuperar filas tipadas
// ============================================================================

static int test_insert_and_recover_single_user(void) {
    Schema s = make_users_schema();
    schema_save(&s, TEST_DIR);

    int rowid = insert_user(TEST_DIR, &s, 42, "Ana", 30, "Barcelona");
    ASSERT(rowid > 0, "insert_user rowid valido");

    int oid; char oname[51]; int oage; char ocity[31];
    ASSERT(recover_user(TEST_DIR, &s, rowid, &oid, oname, &oage, ocity),
           "recover_user retorna exito");
    ASSERT_INT(42,  oid,  "id correcto");
    ASSERT_STR("Ana", oname, "name correcto");
    ASSERT_INT(30,  oage, "age correcto");
    ASSERT_STR("Barcelona", ocity, "city correcto");
    return 1;
}

static int test_insert_and_recover_multiple_users(void) {
    Schema s = make_users_schema();
    schema_save(&s, TEST_DIR);

    int rowids[5];
    const char *names[] = {"Juan", "Ana", "Luis", "Maria", "Carlos"};
    int ages[]          = {25, 30, 22, 28, 35};
    const char *cities[]= {"Madrid", "Barcelona", "Valencia", "Sevilla", "Madrid"};

    for (int i = 0; i < 5; i++)
        rowids[i] = insert_user(TEST_DIR, &s, i+1, names[i], ages[i], cities[i]);

    int oid; char oname[51]; int oage; char ocity[31];
    for (int i = 0; i < 5; i++) {
        ASSERT(recover_user(TEST_DIR, &s, rowids[i], &oid, oname, &oage, ocity),
               "recover_user exito para cada fila");
        ASSERT_INT(i+1,    oid,   "id correcto");
        ASSERT_STR(names[i], oname, "name correcto");
        ASSERT_INT(ages[i],  oage,  "age correcto");
        ASSERT_STR(cities[i],ocity, "city correcto");
    }
    return 1;
}

static int test_insert_and_recover_product(void) {
    Schema s = make_products_schema();
    schema_save(&s, TEST_DIR);

    int rowid = insert_product(TEST_DIR, &s, 1, "Laptop", 1200);
    ASSERT(rowid > 0, "insert_product rowid valido");

    int oid, oprice; char oname[51];
    ASSERT(recover_product(TEST_DIR, &s, rowid, &oid, oname, &oprice),
           "recover_product exito");
    ASSERT_INT(1,        oid,    "product id correcto");
    ASSERT_STR("Laptop", oname,  "product name correcto");
    ASSERT_INT(1200,     oprice, "product price correcto");
    return 1;
}

static int test_recover_correct_row_by_rowid(void) {
    Schema s = make_users_schema();
    schema_save(&s, TEST_DIR);

    // Insertar varias filas y verificar que el rowid apunta a la correcta
    int r1 = insert_user(TEST_DIR, &s, 10, "Pedro", 40, "Bilbao");
    int r2 = insert_user(TEST_DIR, &s, 20, "Sofia", 19, "Zaragoza");
    int r3 = insert_user(TEST_DIR, &s, 30, "Diego", 29, "Malaga");

    int oid; char oname[51]; int oage; char ocity[31];

    recover_user(TEST_DIR, &s, r2, &oid, oname, &oage, ocity);
    ASSERT_INT(20,       oid,   "rowid r2 apunta a id=20");
    ASSERT_STR("Sofia",  oname, "rowid r2 apunta a Sofia");

    recover_user(TEST_DIR, &s, r1, &oid, oname, &oage, ocity);
    ASSERT_INT(10,       oid,   "rowid r1 apunta a id=10");
    ASSERT_STR("Pedro",  oname, "rowid r1 apunta a Pedro");

    recover_user(TEST_DIR, &s, r3, &oid, oname, &oage, ocity);
    ASSERT_INT(30,       oid,   "rowid r3 apunta a id=30");
    ASSERT_STR("Diego",  oname, "rowid r3 apunta a Diego");

    return 1;
}


// ============================================================================
// BLOQUE 3: overflow a múltiples páginas
// ============================================================================

static int test_rows_survive_page_overflow(void) {
    Schema s = make_users_schema();
    schema_save(&s, TEST_DIR);

    // Insertar suficientes filas para forzar page 2
    int rowids[100];
    for (int i = 0; i < 100; i++)
        rowids[i] = insert_user(TEST_DIR, &s, i+1, "UserLargo", 25, "CiudadLargaDeNombre");

    // Verificar que hay más de una página de datos
    int npages = get_num_pages(TEST_DIR, "users");
    ASSERT(npages >= 3, "hay al menos 3 paginas (0=schema, 1+2=datos)");

    // Todas las filas son recuperables
    int all_ok = 1;
    int oid; char oname[51]; int oage; char ocity[31];
    for (int i = 0; i < 100; i++) {
        if (!recover_user(TEST_DIR, &s, rowids[i], &oid, oname, &oage, ocity) ||
            oid != i+1) {
            all_ok = 0; break;
        }
    }
    ASSERT(all_ok, "100 filas recuperables tras overflow a multiples paginas");
    return 1;
}

static int test_page1_rows_intact_after_page2_created(void) {
    Schema s = make_users_schema();
    schema_save(&s, TEST_DIR);

    // Primera fila — va a page 1
    int r_first = insert_user(TEST_DIR, &s, 999, "Primero", 99, "PrimeraCity");
    ASSERT_INT(1, decode_rowid_page(r_first), "primera fila en page 1");

    // Llenar page 1 con filas grandes
    for (int i = 0; i < 50; i++)
        insert_user(TEST_DIR, &s, i, "NombreLargoXXXXXX", 30, "CiudadLargaYYYYY");

    // La primera fila sigue siendo recuperable con los datos correctos
    int oid; char oname[51]; int oage; char ocity[31];
    ASSERT(recover_user(TEST_DIR, &s, r_first, &oid, oname, &oage, ocity),
           "primera fila recuperable tras crear page 2");
    ASSERT_INT(999,       oid,   "id de primera fila intacto");
    ASSERT_STR("Primero", oname, "name de primera fila intacto");
    ASSERT_INT(99,        oage,  "age de primera fila intacto");
    return 1;
}


// ============================================================================
// BLOQUE 4: dos tablas en el mismo directorio
// ============================================================================

static int test_two_tables_schema_isolated(void) {
    Schema su = make_users_schema();
    Schema sp = make_products_schema();

    schema_save(&su, TEST_DIR);
    schema_save(&sp, TEST_DIR);

    // Cargar cada schema por separado
    Schema lu; memset(&lu, 0, sizeof(Schema));
    Schema lp; memset(&lp, 0, sizeof(Schema));
    ASSERT_INT(0, schema_load(&lu, "users",    TEST_DIR), "schema_load users ok");
    ASSERT_INT(0, schema_load(&lp, "products", TEST_DIR), "schema_load products ok");

    ASSERT_STR("users",    lu.table_name, "users table_name correcto");
    ASSERT_STR("products", lp.table_name, "products table_name correcto");
    ASSERT_INT(4, lu.num_columns, "users num_columns == 4");
    ASSERT_INT(3, lp.num_columns, "products num_columns == 3");
    return 1;
}

static int test_two_tables_data_isolated(void) {
    Schema su = make_users_schema();
    Schema sp = make_products_schema();
    schema_save(&su, TEST_DIR);
    schema_save(&sp, TEST_DIR);

    int ru = insert_user(   TEST_DIR, &su, 1, "Juan",   25, "Madrid");
    int rp = insert_product(TEST_DIR, &sp, 1, "Laptop", 1200);

    // Recuperar de cada tabla
    int oid; char oname[51]; int oage; char ocity[31];
    ASSERT(recover_user(TEST_DIR, &su, ru, &oid, oname, &oage, ocity),
           "user recuperable de su tabla");
    ASSERT_STR("Juan", oname, "user name correcto");

    int pid, pprice; char pname[51];
    ASSERT(recover_product(TEST_DIR, &sp, rp, &pid, pname, &pprice),
           "product recuperable de su tabla");
    ASSERT_STR("Laptop", pname, "product name correcto");

    // Los datos no se mezclan
    ASSERT_INT(1, decode_rowid_page(ru), "user en page 1 de users");
    ASSERT_INT(1, decode_rowid_page(rp), "product en page 1 de products");
    return 1;
}

static int test_two_tables_interleaved_inserts(void) {
    Schema su = make_users_schema();
    Schema sp = make_products_schema();
    schema_save(&su, TEST_DIR);
    schema_save(&sp, TEST_DIR);

    // Insertar intercalando las dos tablas
    int ru[5], rp[5];
    for (int i = 0; i < 5; i++) {
        char name[20]; sprintf(name, "User%d", i);
        char pname[20]; sprintf(pname, "Prod%d", i);
        ru[i] = insert_user(   TEST_DIR, &su, i,   name,  20+i, "City");
        rp[i] = insert_product(TEST_DIR, &sp, i+10, pname, 100*i);
    }

    // Verificar que todos son recuperables y correctos
    int all_ok = 1;
    for (int i = 0; i < 5; i++) {
        char exp_uname[20]; sprintf(exp_uname, "User%d", i);
        char exp_pname[20]; sprintf(exp_pname, "Prod%d", i);

        int oid; char oname[51]; int oage; char ocity[31];
        if (!recover_user(TEST_DIR, &su, ru[i], &oid, oname, &oage, ocity) ||
            strcmp(oname, exp_uname) != 0) { all_ok = 0; break; }

        int pid, pprice; char pname[51];
        if (!recover_product(TEST_DIR, &sp, rp[i], &pid, pname, &pprice) ||
            strcmp(pname, exp_pname) != 0) { all_ok = 0; break; }
    }
    ASSERT(all_ok, "inserts intercalados en 2 tablas: todos recuperables y correctos");
    return 1;
}


// ============================================================================
// BLOQUE 5: consistencia end-to-end
// ============================================================================

static int test_schema_column_index_used_to_recover(void) {
    // Verifica que schema_get_column_index devuelve el índice correcto
    // y que ese índice funciona para acceder al valor correcto de la fila
    Schema s = make_users_schema();
    schema_save(&s, TEST_DIR);

    int rowid = insert_user(TEST_DIR, &s, 7, "Elena", 24, "Sevilla");

    int page_id = decode_rowid_page(rowid);
    int slot_id = decode_rowid_slot(rowid);
    char page[PAGE_SIZE];
    load_page(TEST_DIR, s.table_name, page_id, page);
    char *row = read_row(page, slot_id);
    int row_size = get_row_size(page, slot_id);

    int   oid, oage;
    char  oname[51], ocity[31];
    void *vo[] = {&oid, oname, &oage, ocity};
    int   so[4];
    row_deserialize(&s, row, row_size, vo, so);
    oname[so[1]] = '\0';
    ocity[so[3]] = '\0';

    // Usar schema_get_column_index para verificar qué columna es qué
    int idx_id   = schema_get_column_index(&s, "id");
    int idx_name = schema_get_column_index(&s, "name");
    int idx_age  = schema_get_column_index(&s, "age");
    int idx_city = schema_get_column_index(&s, "city");

    ASSERT_INT(0, idx_id,   "get_column_index id == 0");
    ASSERT_INT(1, idx_name, "get_column_index name == 1");
    ASSERT_INT(2, idx_age,  "get_column_index age == 2");
    ASSERT_INT(3, idx_city, "get_column_index city == 3");

    // Los valores en esos índices son correctos
    ASSERT_INT(7,   oid,  "id en indice 0 correcto");
    ASSERT_STR("Elena", oname, "name en indice 1 correcto");
    ASSERT_INT(24,  oage, "age en indice 2 correcto");
    ASSERT_STR("Sevilla", ocity, "city en indice 3 correcto");
    return 1;
}

static int test_full_pipeline_save_insert_reload_recover(void) {
    // Pipeline completo:
    // 1. Definir schema
    // 2. schema_save
    // 3. Insertar filas
    // 4. schema_load (simula reinicio del sistema)
    // 5. Recuperar filas con el schema recargado

    Schema orig = make_users_schema();
    schema_save(&orig, TEST_DIR);

    int r1 = insert_user(TEST_DIR, &orig, 1, "Carlos", 35, "Madrid");
    int r2 = insert_user(TEST_DIR, &orig, 2, "Laura",  27, "Bilbao");

    // Simular reinicio: cargar el schema desde disco
    Schema reloaded; memset(&reloaded, 0, sizeof(Schema));
    ASSERT_INT(0, schema_load(&reloaded, "users", TEST_DIR),
               "schema_load tras inserts retorna 0");

    // Recuperar con el schema recargado
    int oid; char oname[51]; int oage; char ocity[31];

    ASSERT(recover_user(TEST_DIR, &reloaded, r1, &oid, oname, &oage, ocity),
           "fila 1 recuperable con schema recargado");
    ASSERT_INT(1,        oid,   "fila 1 id correcto con schema recargado");
    ASSERT_STR("Carlos", oname, "fila 1 name correcto con schema recargado");

    ASSERT(recover_user(TEST_DIR, &reloaded, r2, &oid, oname, &oage, ocity),
           "fila 2 recuperable con schema recargado");
    ASSERT_INT(2,       oid,   "fila 2 id correcto con schema recargado");
    ASSERT_STR("Laura", oname, "fila 2 name correcto con schema recargado");

    return 1;
}

static int test_stress_50_users_all_correct(void) {
    Schema s = make_users_schema();
    schema_save(&s, TEST_DIR);

    int rowids[50];
    for (int i = 0; i < 50; i++) {
        char name[20]; sprintf(name, "User%02d", i);
        rowids[i] = insert_user(TEST_DIR, &s, i+1, name, 18+i, "Madrid");
    }

    int all_ok = 1;
    for (int i = 0; i < 50; i++) {
        char exp[20]; sprintf(exp, "User%02d", i);
        int oid; char oname[51]; int oage; char ocity[31];
        if (!recover_user(TEST_DIR, &s, rowids[i], &oid, oname, &oage, ocity) ||
            oid != i+1 || strcmp(oname, exp) != 0 || oage != 18+i) {
            printf("    FAIL: fila %d incorrecta\n", i);
            all_ok = 0; break;
        }
    }
    ASSERT(all_ok, "50 users insertados y recuperados correctamente");

    // Schema sigue intacto después de todo
    Schema loaded; memset(&loaded, 0, sizeof(Schema));
    schema_load(&loaded, "users", TEST_DIR);
    ASSERT_INT(4, loaded.num_columns, "schema intacto tras 50 inserts");
    return 1;
}


// ============================================================================
// MAIN
// ============================================================================

int main(void) {
    strcpy(TEST_DIR, "/tmp/test_integration_XXXXXX");
    if (!mkdtemp(TEST_DIR)) { perror("mkdtemp"); return 1; }

    printf("=== TEST INTEGRATION (schema + heap) ===\n");
    printf("Directorio temporal: %s\n\n", TEST_DIR);

    printf("-- Bloque 1: schema en page 0, datos en page 1+ --\n");
    RUN_TEST(test_schema_page0_data_page1);
    RUN_TEST(test_schema_not_overwritten_by_inserts);

    printf("\n-- Bloque 2: insertar y recuperar filas tipadas --\n");
    RUN_TEST(test_insert_and_recover_single_user);
    RUN_TEST(test_insert_and_recover_multiple_users);
    RUN_TEST(test_insert_and_recover_product);
    RUN_TEST(test_recover_correct_row_by_rowid);

    printf("\n-- Bloque 3: overflow a multiples paginas --\n");
    RUN_TEST(test_rows_survive_page_overflow);
    RUN_TEST(test_page1_rows_intact_after_page2_created);

    printf("\n-- Bloque 4: dos tablas en el mismo directorio --\n");
    RUN_TEST(test_two_tables_schema_isolated);
    RUN_TEST(test_two_tables_data_isolated);
    RUN_TEST(test_two_tables_interleaved_inserts);

    printf("\n-- Bloque 5: pipeline end-to-end --\n");
    RUN_TEST(test_schema_column_index_used_to_recover);
    RUN_TEST(test_full_pipeline_save_insert_reload_recover);
    RUN_TEST(test_stress_50_users_all_correct);

    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DIR);
    system(cmd);

    printf("\n=== RESULTADO: %d passed, %d failed, %d total ===\n",
           tests_passed, tests_failed, tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
