// test_schema.c — Test exhaustivo del módulo schema
// Compilar: gcc -Wall -Wextra -g -o test_schema test_schema.c ../schema.c ../disk.c ../page.c ../heap.c
// Ejecutar: ./test_schema

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../schema.h"
#include "../disk.h"

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
// Helpers para construir schemas de test
// ============================================================================

static Schema make_schema_4types(void) {
    Schema s;
    memset(&s, 0, sizeof(Schema));
    strcpy(s.table_name, "test_table");
    s.num_columns = 4;
    strcpy(s.columns[0].name, "id");
    s.columns[0].type = TYPE_INT;     s.columns[0].max_size = 4;
    s.columns[0].nullable = 0;        s.columns[0].is_primary_key = 1;
    strcpy(s.columns[1].name, "value");
    s.columns[1].type = TYPE_FLOAT;   s.columns[1].max_size = 4;
    s.columns[1].nullable = 1;        s.columns[1].is_primary_key = 0;
    strcpy(s.columns[2].name, "flag");
    s.columns[2].type = TYPE_BOOLEAN; s.columns[2].max_size = 1;
    s.columns[2].nullable = 1;        s.columns[2].is_primary_key = 0;
    strcpy(s.columns[3].name, "name");
    s.columns[3].type = TYPE_VARCHAR; s.columns[3].max_size = 50;
    s.columns[3].nullable = 1;        s.columns[3].is_primary_key = 0;
    return s;
}

static Schema make_schema_max_columns(void) {
    Schema s;
    memset(&s, 0, sizeof(Schema));
    strcpy(s.table_name, "max_cols_table");
    s.num_columns = MAX_COLUMNS;
    for (int i = 0; i < MAX_COLUMNS; i++) {
        sprintf(s.columns[i].name, "col%d", i);
        s.columns[i].type     = (ColumnType)(i % 4);
        s.columns[i].max_size = (i % 4 == TYPE_VARCHAR) ? 50 : 4;
        s.columns[i].nullable      = (i % 2);
        s.columns[i].is_primary_key = (i == 0);
    }
    return s;
}

static Schema make_schema_int3(void) {
    Schema s;
    memset(&s, 0, sizeof(Schema));
    strcpy(s.table_name, "int3");
    s.num_columns = 3;
    strcpy(s.columns[0].name, "zero"); s.columns[0].type = TYPE_INT;
    strcpy(s.columns[1].name, "neg");  s.columns[1].type = TYPE_INT;
    strcpy(s.columns[2].name, "max");  s.columns[2].type = TYPE_INT;
    for (int i = 0; i < 3; i++) s.columns[i].max_size = 4;
    return s;
}

static Schema make_schema_float2(void) {
    Schema s;
    memset(&s, 0, sizeof(Schema));
    strcpy(s.table_name, "float2");
    s.num_columns = 2;
    strcpy(s.columns[0].name, "a"); s.columns[0].type = TYPE_FLOAT; s.columns[0].max_size = 4;
    strcpy(s.columns[1].name, "b"); s.columns[1].type = TYPE_FLOAT; s.columns[1].max_size = 4;
    return s;
}

static Schema make_schema_bool2(void) {
    Schema s;
    memset(&s, 0, sizeof(Schema));
    strcpy(s.table_name, "bool2");
    s.num_columns = 2;
    strcpy(s.columns[0].name, "t"); s.columns[0].type = TYPE_BOOLEAN; s.columns[0].max_size = 1;
    strcpy(s.columns[1].name, "f"); s.columns[1].type = TYPE_BOOLEAN; s.columns[1].max_size = 1;
    return s;
}

static Schema make_schema_varchar1(int max_size) {
    Schema s;
    memset(&s, 0, sizeof(Schema));
    strcpy(s.table_name, "vc1");
    s.num_columns = 1;
    strcpy(s.columns[0].name, "data");
    s.columns[0].type = TYPE_VARCHAR;
    s.columns[0].max_size = max_size;
    return s;
}

static Schema make_schema_9cols(void) {
    Schema s;
    memset(&s, 0, sizeof(Schema));
    strcpy(s.table_name, "nine");
    s.num_columns = 9;
    for (int i = 0; i < 9; i++) {
        sprintf(s.columns[i].name, "c%d", i);
        s.columns[i].type = TYPE_INT;
        s.columns[i].max_size = 4;
    }
    return s;
}


// ============================================================================
// BLOQUE 1: schema_serialize / schema_deserialize
// ============================================================================

static int test_serialize_deserialize_4types(void) {
    Schema orig = make_schema_4types();
    char buf[PAGE_SIZE];
    ASSERT_INT(0, schema_serialize(&orig, buf), "schema_serialize retorna 0");
    Schema loaded; memset(&loaded, 0, sizeof(Schema));
    ASSERT_INT(0, schema_deserialize(&loaded, buf), "schema_deserialize retorna 0");
    ASSERT_STR(orig.table_name, loaded.table_name, "table_name coincide");
    ASSERT_INT(orig.num_columns, loaded.num_columns, "num_columns coincide");
    for (int i = 0; i < orig.num_columns; i++) {
        ASSERT(strcmp(loaded.columns[i].name, orig.columns[i].name) == 0,
               "nombre de columna coincide");
        ASSERT_INT(orig.columns[i].type,          loaded.columns[i].type,          "type coincide");
        ASSERT_INT(orig.columns[i].max_size,       loaded.columns[i].max_size,      "max_size coincide");
        ASSERT_INT(orig.columns[i].nullable,       loaded.columns[i].nullable,      "nullable coincide");
        ASSERT_INT(orig.columns[i].is_primary_key, loaded.columns[i].is_primary_key,"is_pk coincide");
    }
    return 1;
}

static int test_serialize_deserialize_max_columns(void) {
    Schema orig = make_schema_max_columns();
    char buf[PAGE_SIZE];
    ASSERT_INT(0, schema_serialize(&orig, buf), "serialize MAX_COLUMNS retorna 0");
    Schema loaded; memset(&loaded, 0, sizeof(Schema));
    ASSERT_INT(0, schema_deserialize(&loaded, buf), "deserialize MAX_COLUMNS retorna 0");
    ASSERT_INT(MAX_COLUMNS, loaded.num_columns, "num_columns == MAX_COLUMNS");
    for (int i = 0; i < MAX_COLUMNS; i++) {
        ASSERT(strcmp(loaded.columns[i].name, orig.columns[i].name) == 0,
               "nombre de columna max coincide");
        ASSERT_INT(orig.columns[i].type, loaded.columns[i].type, "type max coincide");
        ASSERT_INT(orig.columns[i].max_size, loaded.columns[i].max_size, "max_size max coincide");
    }
    return 1;
}

static int test_serialize_empty_schema(void) {
    Schema empty; memset(&empty, 0, sizeof(Schema));
    strcpy(empty.table_name, "empty_table");
    empty.num_columns = 0;
    char buf[PAGE_SIZE];
    ASSERT_INT(0, schema_serialize(&empty, buf), "serialize schema vacio retorna 0");
    Schema loaded; memset(&loaded, 0, sizeof(Schema));
    ASSERT_INT(0, schema_deserialize(&loaded, buf), "deserialize schema vacio retorna 0");
    ASSERT_INT(0, loaded.num_columns, "num_columns == 0");
    ASSERT_STR("empty_table", loaded.table_name, "table_name vacio coincide");
    return 1;
}

static int test_serialize_null_pointers(void) {
    char buf[PAGE_SIZE];
    ASSERT_INT(-1, schema_serialize(NULL, buf),  "serialize NULL schema retorna -1");
    ASSERT_INT(-1, schema_serialize(NULL, NULL), "serialize NULL,NULL retorna -1");
    return 1;
}

static int test_deserialize_null_pointers(void) {
    char buf[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    ASSERT_INT(-1, schema_deserialize(NULL, buf),  "deserialize NULL schema retorna -1");
    ASSERT_INT(-1, schema_deserialize(NULL, NULL), "deserialize NULL,NULL retorna -1");
    return 1;
}

static int test_deserialize_invalid_num_columns(void) {
    char buf[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    strcpy(buf, "test_table");
    int bad_num = MAX_COLUMNS + 1;
    memcpy(buf + MAX_TABLE_NAME, &bad_num, sizeof(int));
    Schema s; memset(&s, 0, sizeof(Schema));
    ASSERT_INT(-1, schema_deserialize(&s, buf),
               "deserialize num_columns > MAX_COLUMNS retorna -1");
    return 1;
}

static int test_serialize_deserialize_varchar_max_size(void) {
    Schema s = make_schema_varchar1(255);
    char buf[PAGE_SIZE];
    ASSERT_INT(0, schema_serialize(&s, buf), "serialize VARCHAR max_size=255 retorna 0");
    Schema loaded; memset(&loaded, 0, sizeof(Schema));
    schema_deserialize(&loaded, buf);
    ASSERT_INT(255, loaded.columns[0].max_size, "max_size 255 se preserva");
    return 1;
}

static int test_serialize_preserves_nullable_and_pk(void) {
    Schema orig = make_schema_4types();
    char buf[PAGE_SIZE];
    schema_serialize(&orig, buf);
    Schema loaded; memset(&loaded, 0, sizeof(Schema));
    schema_deserialize(&loaded, buf);
    ASSERT_INT(0, loaded.columns[0].nullable,       "col 0 nullable == 0");
    ASSERT_INT(1, loaded.columns[0].is_primary_key, "col 0 is_pk == 1");
    ASSERT_INT(1, loaded.columns[1].nullable,       "col 1 nullable == 1");
    ASSERT_INT(0, loaded.columns[1].is_primary_key, "col 1 is_pk == 0");
    return 1;
}


// ============================================================================
// BLOQUE 2: schema_save / schema_load
// ============================================================================

static int test_save_load_roundtrip(void) {
    Schema orig = make_schema_4types();
    ASSERT_INT(0, schema_save(&orig, TEST_DIR), "schema_save retorna 0");
    Schema loaded; memset(&loaded, 0, sizeof(Schema));
    ASSERT_INT(0, schema_load(&loaded, "test_table", TEST_DIR), "schema_load retorna 0");
    ASSERT_STR(orig.table_name, loaded.table_name, "save/load table_name coincide");
    ASSERT_INT(orig.num_columns, loaded.num_columns, "save/load num_columns coincide");
    for (int i = 0; i < orig.num_columns; i++) {
        ASSERT(strcmp(loaded.columns[i].name, orig.columns[i].name) == 0,
               "save/load nombre columna coincide");
        ASSERT_INT(orig.columns[i].type, loaded.columns[i].type, "save/load type coincide");
    }
    return 1;
}

static int test_load_nonexistent_returns_minus1(void) {
    Schema s; memset(&s, 0, sizeof(Schema));
    ASSERT_INT(-1, schema_load(&s, "tabla_inexistente", TEST_DIR),
               "schema_load tabla inexistente retorna -1");
    return 1;
}

static int test_save_null_pointers(void) {
    Schema s = make_schema_4types();
    ASSERT_INT(-1, schema_save(NULL, TEST_DIR), "schema_save NULL schema retorna -1");
    ASSERT_INT(-1, schema_save(&s, NULL),       "schema_save NULL dir retorna -1");
    return 1;
}

static int test_load_null_pointers(void) {
    Schema s; memset(&s, 0, sizeof(Schema));
    ASSERT_INT(-1, schema_load(&s,   "t", NULL),     "schema_load NULL dir retorna -1");
    ASSERT_INT(-1, schema_load(&s,   NULL, TEST_DIR),"schema_load NULL name retorna -1");
    ASSERT_INT(-1, schema_load(NULL, "t", TEST_DIR), "schema_load NULL schema retorna -1");
    return 1;
}

static int test_save_writes_to_page_0(void) {
    Schema orig = make_schema_4types();
    schema_save(&orig, TEST_DIR);
    // Verificar que el archivo .db existe y tiene al menos 1 página
    ASSERT(get_num_pages(TEST_DIR, "test_table") >= 1,
           "schema_save crea archivo .db con al menos 1 pagina");
    return 1;
}

static int test_save_load_max_columns(void) {
    Schema orig = make_schema_max_columns();
    ASSERT_INT(0, schema_save(&orig, TEST_DIR), "schema_save MAX_COLUMNS retorna 0");
    Schema loaded; memset(&loaded, 0, sizeof(Schema));
    ASSERT_INT(0, schema_load(&loaded, "max_cols_table", TEST_DIR),
               "schema_load MAX_COLUMNS retorna 0");
    ASSERT_INT(MAX_COLUMNS, loaded.num_columns, "num_columns MAX_COLUMNS tras save/load");
    for (int i = 0; i < MAX_COLUMNS; i++)
        ASSERT(strcmp(loaded.columns[i].name, orig.columns[i].name) == 0,
               "nombre columna max tras save/load coincide");
    return 1;
}


// ============================================================================
// BLOQUE 3: schema_get_column_index
// ============================================================================

static int test_get_column_index_basic(void) {
    Schema s = make_schema_4types();
    ASSERT_INT(0, schema_get_column_index(&s, "id"),    "id -> 0");
    ASSERT_INT(1, schema_get_column_index(&s, "value"), "value -> 1");
    ASSERT_INT(2, schema_get_column_index(&s, "flag"),  "flag -> 2");
    ASSERT_INT(3, schema_get_column_index(&s, "name"),  "name -> 3");
    return 1;
}

static int test_get_column_index_nonexistent(void) {
    Schema s = make_schema_4types();
    ASSERT_INT(-1, schema_get_column_index(&s, "nope"),      "columna inexistente -> -1");
    ASSERT_INT(-1, schema_get_column_index(&s, "id_"),       "id_ no existe -> -1");
    ASSERT_INT(-1, schema_get_column_index(&s, ""),          "string vacio -> -1");
    ASSERT_INT(-1, schema_get_column_index(&s, "NAME"),      "case sensitive: NAME != name");
    return 1;
}

static int test_get_column_index_null_pointers(void) {
    Schema s; memset(&s, 0, sizeof(Schema));
    ASSERT_INT(-1, schema_get_column_index(NULL, "col"), "NULL schema -> -1");
    ASSERT_INT(-1, schema_get_column_index(&s, NULL),    "NULL col_name -> -1");
    return 1;
}

static int test_get_column_index_first_and_last(void) {
    Schema s = make_schema_max_columns();
    char first_name[16], last_name[16];
    sprintf(first_name, "col0");
    sprintf(last_name,  "col%d", MAX_COLUMNS - 1);
    ASSERT_INT(0,              schema_get_column_index(&s, first_name), "primera columna -> 0");
    ASSERT_INT(MAX_COLUMNS-1,  schema_get_column_index(&s, last_name),  "ultima columna -> MAX_COLUMNS-1");
    return 1;
}


// ============================================================================
// BLOQUE 4: row_serialize / row_deserialize — tipos básicos
// ============================================================================

static int test_row_roundtrip_all_types(void) {
    Schema s = make_schema_4types();
    int   iv = 42;
    float fv = 3.14f;
    char  bv = 1;
    char  sv[] = "hola";
    void *values[] = {&iv, &fv, &bv, sv};
    int   sizes[]  = {4, 4, 1, 4};
    char buf[PAGE_SIZE]; int out_size;
    ASSERT_INT(0, row_serialize(&s, values, sizes, buf, PAGE_SIZE, &out_size),
               "row_serialize 4 tipos retorna 0");
    ASSERT(out_size > 0, "out_size > 0");

    int   ri; float rf; char rb; char rs[50];
    void *vo[] = {&ri, &rf, &rb, rs};
    int   so[4];
    int r = row_deserialize(&s, buf, out_size, vo, so);
    ASSERT(r > 0,                               "row_deserialize retorna bytes > 0");
    ASSERT_INT(42,   ri,                        "INT roundtrip correcto");
    ASSERT(rf > 3.13f && rf < 3.15f,            "FLOAT roundtrip correcto");
    ASSERT_INT(1,    (int)rb,                   "BOOLEAN roundtrip correcto");
    ASSERT_INT(4,    so[3],                     "VARCHAR size correcto");
    ASSERT_MEM("hola", rs, 4,                   "VARCHAR data correcto");
    return 1;
}

static int test_row_int_edge_cases(void) {
    Schema s = make_schema_int3();
    int vz = 0, vn = -2147483648, vm = 2147483647;
    void *v[] = {&vz, &vn, &vm};
    int sz[] = {4, 4, 4};
    char buf[PAGE_SIZE]; int out_size;
    row_serialize(&s, v, sz, buf, PAGE_SIZE, &out_size);
    int oz, on, om; void *vo[] = {&oz, &on, &om}; int so[3];
    row_deserialize(&s, buf, out_size, vo, so);
    ASSERT_INT(0,           oz, "INT 0 roundtrip");
    ASSERT_INT(-2147483648, on, "INT_MIN roundtrip");
    ASSERT_INT(2147483647,  om, "INT_MAX roundtrip");
    return 1;
}

static int test_row_float_edge_cases(void) {
    Schema s = make_schema_float2();
    float va = 0.0f, vb = -123.456f;
    void *v[] = {&va, &vb}; int sz[] = {4, 4};
    char buf[PAGE_SIZE]; int out_size;
    row_serialize(&s, v, sz, buf, PAGE_SIZE, &out_size);
    float oa, ob; void *vo[] = {&oa, &ob}; int so[2];
    row_deserialize(&s, buf, out_size, vo, so);
    ASSERT(oa == 0.0f, "FLOAT 0.0 roundtrip");
    float diff = ob - (-123.456f); if (diff < 0) diff = -diff;
    ASSERT(diff < 0.001f, "FLOAT -123.456 roundtrip dentro de tolerancia");
    return 1;
}

static int test_row_boolean_edge_cases(void) {
    Schema s = make_schema_bool2();
    char vt = 1, vf = 0;
    void *v[] = {&vt, &vf}; int sz[] = {1, 1};
    char buf[PAGE_SIZE]; int out_size;
    row_serialize(&s, v, sz, buf, PAGE_SIZE, &out_size);
    char ot = 2, of = 2; void *vo[] = {&ot, &of}; int so[2];
    row_deserialize(&s, buf, out_size, vo, so);
    ASSERT_INT(1, (int)ot, "BOOLEAN true roundtrip");
    ASSERT_INT(0, (int)of, "BOOLEAN false roundtrip");
    return 1;
}

static int test_row_varchar_255_bytes(void) {
    Schema s = make_schema_varchar1(255);
    char val[255]; memset(val, 'X', 255);
    void *v[] = {val}; int sz[] = {255};
    char buf[PAGE_SIZE]; int out_size;
    ASSERT_INT(0, row_serialize(&s, v, sz, buf, PAGE_SIZE, &out_size),
               "row_serialize VARCHAR 255 retorna 0");
    char out[255]; void *vo[] = {out}; int so[1];
    row_deserialize(&s, buf, out_size, vo, so);
    ASSERT_INT(255, so[0], "VARCHAR 255 size correcto tras deserialize");
    int all_x = 1;
    for (int i = 0; i < 255; i++) if (out[i] != 'X') { all_x = 0; break; }
    ASSERT(all_x, "VARCHAR 255 bytes todos son 'X'");
    return 1;
}

static int test_row_varchar_empty_string(void) {
    Schema s = make_schema_varchar1(50);
    char val[] = "";
    void *v[] = {val}; int sz[] = {0};
    char buf[PAGE_SIZE]; int out_size;
    ASSERT_INT(0, row_serialize(&s, v, sz, buf, PAGE_SIZE, &out_size),
               "row_serialize VARCHAR vacio retorna 0");
    char out[50]; void *vo[] = {out}; int so[1];
    row_deserialize(&s, buf, out_size, vo, so);
    ASSERT_INT(0, so[0], "VARCHAR vacio size == 0 tras deserialize");
    return 1;
}


// ============================================================================
// BLOQUE 5: row_serialize / row_deserialize — valores NULL
// ============================================================================

static int test_row_all_nulls(void) {
    Schema s = make_schema_4types();
    int d = 0; char dv[50];
    void *v[] = {&d, dv, &d, dv}; int sz[] = {0, 0, 0, 0};
    char buf[PAGE_SIZE]; int out_size;
    ASSERT_INT(0, row_serialize(&s, v, sz, buf, PAGE_SIZE, &out_size),
               "row_serialize todo NULL retorna 0");
    int ri; float rf; char rb; char rs[50];
    void *vo[] = {&ri, &rf, &rb, rs}; int so[4];
    int r = row_deserialize(&s, buf, out_size, vo, so);
    ASSERT(r > 0, "row_deserialize todo NULL retorna bytes > 0");
    for (int i = 0; i < 4; i++)
        ASSERT_INT(0, so[i], "size de columna NULL == 0");
    return 1;
}

static int test_row_partial_nulls(void) {
    Schema s = make_schema_4types();
    int iv = 99; char dv[50];
    // col 0 no null, cols 1-3 null
    void *v[] = {&iv, dv, dv, dv}; int sz[] = {4, 0, 0, 0};
    char buf[PAGE_SIZE]; int out_size;
    ASSERT_INT(0, row_serialize(&s, v, sz, buf, PAGE_SIZE, &out_size),
               "row_serialize parcialmente NULL retorna 0");
    int ri; float rf; char rb; char rs[50];
    void *vo[] = {&ri, &rf, &rb, rs}; int so[4];
    row_deserialize(&s, buf, out_size, vo, so);
    ASSERT_INT(4, so[0], "col 0 no NULL: size == 4");
    ASSERT_INT(99, ri,   "col 0 valor correcto");
    ASSERT_INT(0, so[1], "col 1 NULL: size == 0");
    ASSERT_INT(0, so[2], "col 2 NULL: size == 0");
    ASSERT_INT(0, so[3], "col 3 NULL: size == 0");
    return 1;
}


// ============================================================================
// BLOQUE 6: row_serialize / row_deserialize — null bitmap con 9+ columnas
// ============================================================================

static int test_row_9cols_2byte_bitmap(void) {
    Schema s = make_schema_9cols();
    int vals[] = {1,2,3,4,5,6,7,8,9};
    void *v[] = {&vals[0],&vals[1],&vals[2],&vals[3],&vals[4],
                 &vals[5],&vals[6],&vals[7],&vals[8]};
    int sz[] = {4,4,4,4,4,4,4,4,4};
    char buf[PAGE_SIZE]; int out_size;
    ASSERT_INT(0, row_serialize(&s, v, sz, buf, PAGE_SIZE, &out_size),
               "row_serialize 9 cols (bitmap 2 bytes) retorna 0");
    int ov[9]; void *vo[] = {&ov[0],&ov[1],&ov[2],&ov[3],&ov[4],
                              &ov[5],&ov[6],&ov[7],&ov[8]};
    int so[9];
    int r = row_deserialize(&s, buf, out_size, vo, so);
    ASSERT(r > 0, "row_deserialize 9 cols retorna > 0");
    for (int i = 0; i < 9; i++)
        ASSERT_INT(i+1, ov[i], "valor de columna en schema de 9 cols correcto");
    return 1;
}

static int test_row_9cols_with_nulls(void) {
    Schema s = make_schema_9cols();
    int vals[9]; for (int i = 0; i < 9; i++) vals[i] = i + 10;
    void *v[] = {&vals[0],&vals[1],&vals[2],&vals[3],&vals[4],
                 &vals[5],&vals[6],&vals[7],&vals[8]};
    // Poner NULL en columnas 0, 4 y 8 (primera, media y última)
    int sz[] = {0,4,4,4,0,4,4,4,0};
    char buf[PAGE_SIZE]; int out_size;
    ASSERT_INT(0, row_serialize(&s, v, sz, buf, PAGE_SIZE, &out_size),
               "row_serialize 9 cols con NULLs retorna 0");
    int ov[9]; void *vo[] = {&ov[0],&ov[1],&ov[2],&ov[3],&ov[4],
                              &ov[5],&ov[6],&ov[7],&ov[8]};
    int so[9];
    row_deserialize(&s, buf, out_size, vo, so);
    ASSERT_INT(0, so[0], "col 0 NULL: size == 0");
    ASSERT_INT(4, so[1], "col 1 no NULL: size == 4");
    ASSERT_INT(0, so[4], "col 4 NULL: size == 0");
    ASSERT_INT(0, so[8], "col 8 NULL: size == 0");
    return 1;
}


// ============================================================================
// BLOQUE 7: row_serialize — errores
// ============================================================================

static int test_row_serialize_buffer_too_small(void) {
    Schema s = make_schema_4types();
    int v1 = 1; char v2[10] = "test";
    void *values[] = {&v1, v2, &v1, v2}; int sizes[] = {4,4,1,4};
    char small_buf[2]; int out_size;
    ASSERT_INT(-1, row_serialize(&s, values, sizes, small_buf, 2, &out_size),
               "row_serialize buffer demasiado pequeño retorna -1");
    return 1;
}

static int test_row_deserialize_truncated_buffer(void) {
    Schema s = make_schema_4types();
    char corrupt[1] = {0}; int so[4];
    int ri; float rf; char rb; char rs[50];
    void *vo[] = {&ri, &rf, &rb, rs};
    ASSERT_INT(-1, row_deserialize(&s, corrupt, 1, vo, so),
               "row_deserialize buffer truncado retorna -1");
    return 1;
}


// ============================================================================
// MAIN
// ============================================================================

int main(void) {
    strcpy(TEST_DIR, "/tmp/test_schema_XXXXXX");
    if (!mkdtemp(TEST_DIR)) { perror("mkdtemp"); return 1; }

    printf("=== TEST SCHEMA ===\n");
    printf("Directorio temporal: %s\n\n", TEST_DIR);

    printf("-- Bloque 1: schema_serialize / schema_deserialize --\n");
    RUN_TEST(test_serialize_deserialize_4types);
    RUN_TEST(test_serialize_deserialize_max_columns);
    RUN_TEST(test_serialize_empty_schema);
    RUN_TEST(test_serialize_null_pointers);
    RUN_TEST(test_deserialize_null_pointers);
    RUN_TEST(test_deserialize_invalid_num_columns);
    RUN_TEST(test_serialize_deserialize_varchar_max_size);
    RUN_TEST(test_serialize_preserves_nullable_and_pk);

    printf("\n-- Bloque 2: schema_save / schema_load --\n");
    RUN_TEST(test_save_load_roundtrip);
    RUN_TEST(test_load_nonexistent_returns_minus1);
    RUN_TEST(test_save_null_pointers);
    RUN_TEST(test_load_null_pointers);
    RUN_TEST(test_save_writes_to_page_0);
    RUN_TEST(test_save_load_max_columns);

    printf("\n-- Bloque 3: schema_get_column_index --\n");
    RUN_TEST(test_get_column_index_basic);
    RUN_TEST(test_get_column_index_nonexistent);
    RUN_TEST(test_get_column_index_null_pointers);
    RUN_TEST(test_get_column_index_first_and_last);

    printf("\n-- Bloque 4: row_serialize / row_deserialize tipos --\n");
    RUN_TEST(test_row_roundtrip_all_types);
    RUN_TEST(test_row_int_edge_cases);
    RUN_TEST(test_row_float_edge_cases);
    RUN_TEST(test_row_boolean_edge_cases);
    RUN_TEST(test_row_varchar_255_bytes);
    RUN_TEST(test_row_varchar_empty_string);

    printf("\n-- Bloque 5: row_serialize / row_deserialize NULLs --\n");
    RUN_TEST(test_row_all_nulls);
    RUN_TEST(test_row_partial_nulls);

    printf("\n-- Bloque 6: null bitmap con 9+ columnas --\n");
    RUN_TEST(test_row_9cols_2byte_bitmap);
    RUN_TEST(test_row_9cols_with_nulls);

    printf("\n-- Bloque 7: errores --\n");
    RUN_TEST(test_row_serialize_buffer_too_small);
    RUN_TEST(test_row_deserialize_truncated_buffer);

    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DIR);
    system(cmd);

    printf("\n=== RESULTADO: %d passed, %d failed, %d total ===\n",
           tests_passed, tests_failed, tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}