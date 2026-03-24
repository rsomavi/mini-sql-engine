#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../schema.h"
#include "../disk.h"

#define TEST_DIR "./test_tmp"

static int tests_passed = 0;
static int tests_total = 0;

#define TEST(name, expr) do { \
    tests_total++; \
    if (expr) { \
        printf("PASS: %s\n", name); \
        tests_passed++; \
    } else { \
        printf("FAIL: %s\n", name); \
    } \
} while(0)

// Helper to create a schema with all 4 types
static Schema create_test_schema(void) {
    Schema schema;
    memset(&schema, 0, sizeof(Schema));
    
    strcpy(schema.table_name, "test_table");
    schema.num_columns = 4;
    
    // Column 0: id (INT)
    strcpy(schema.columns[0].name, "id");
    schema.columns[0].type = TYPE_INT;
    schema.columns[0].max_size = 4;
    schema.columns[0].nullable = 0;
    schema.columns[0].is_primary_key = 1;
    
    // Column 1: value (FLOAT)
    strcpy(schema.columns[1].name, "value");
    schema.columns[1].type = TYPE_FLOAT;
    schema.columns[1].max_size = 4;
    schema.columns[1].nullable = 1;
    schema.columns[1].is_primary_key = 0;
    
    // Column 2: flag (BOOLEAN)
    strcpy(schema.columns[2].name, "flag");
    schema.columns[2].type = TYPE_BOOLEAN;
    schema.columns[2].max_size = 1;
    schema.columns[2].nullable = 1;
    schema.columns[2].is_primary_key = 0;
    
    // Column 3: name (VARCHAR)
    strcpy(schema.columns[3].name, "name");
    schema.columns[3].type = TYPE_VARCHAR;
    schema.columns[3].max_size = 50;
    schema.columns[3].nullable = 1;
    schema.columns[3].is_primary_key = 0;
    
    return schema;
}

// Helper to create a schema with max columns (32)
static Schema create_max_columns_schema(void) {
    Schema schema;
    memset(&schema, 0, sizeof(Schema));
    
    strcpy(schema.table_name, "max_cols_table");
    schema.num_columns = 32;
    
    for (int i = 0; i < 32; i++) {
        char name[16];
        sprintf(name, "col%d", i);
        strcpy(schema.columns[i].name, name);
        schema.columns[i].type = (i % 4);  // Cycle through types
        schema.columns[i].max_size = (i % 4 == TYPE_VARCHAR) ? 50 : 4;
        schema.columns[i].nullable = (i % 2);
        schema.columns[i].is_primary_key = (i == 0);
    }
    
    return schema;
}

int main(void) {
    printf("=== Schema Tests ===\n\n");
    
    // Create test directory
    system("rm -rf " TEST_DIR);
    system("mkdir -p " TEST_DIR);
    
    // Test 1: serialize and deserialize schema with all 4 types
    {
        Schema orig = create_test_schema();
        char page_buf[PAGE_SIZE];
        
        int result = schema_serialize(&orig, page_buf);
        TEST("schema_serialize with all 4 types", result == 0);
        
        Schema loaded;
        memset(&loaded, 0, sizeof(Schema));
        result = schema_deserialize(&loaded, page_buf);
        TEST("schema_deserialize returns 0", result == 0);
        
        // Verify table_name
        TEST("table_name matches", strcmp(loaded.table_name, orig.table_name) == 0);
        
        // Verify num_columns
        TEST("num_columns matches", loaded.num_columns == orig.num_columns);
        
        // Verify each column
        for (int i = 0; i < orig.num_columns; i++) {
            char test_name[64];
            sprintf(test_name, "column %d name matches", i);
            TEST(test_name, strcmp(loaded.columns[i].name, orig.columns[i].name) == 0);
            
            sprintf(test_name, "column %d type matches", i);
            TEST(test_name, loaded.columns[i].type == orig.columns[i].type);
            
            sprintf(test_name, "column %d max_size matches", i);
            TEST(test_name, loaded.columns[i].max_size == orig.columns[i].max_size);
            
            sprintf(test_name, "column %d nullable matches", i);
            TEST(test_name, loaded.columns[i].nullable == orig.columns[i].nullable);
            
            sprintf(test_name, "column %d is_primary_key matches", i);
            TEST(test_name, loaded.columns[i].is_primary_key == orig.columns[i].is_primary_key);
        }
    }
    
    // Test 2: schema with maximum columns (32) serializes and deserializes correctly
    {
        Schema orig = create_max_columns_schema();
        char page_buf[PAGE_SIZE];
        
        int result = schema_serialize(&orig, page_buf);
        TEST("schema_serialize with max columns", result == 0);
        
        Schema loaded;
        memset(&loaded, 0, sizeof(Schema));
        result = schema_deserialize(&loaded, page_buf);
        TEST("schema_deserialize max columns returns 0", result == 0);
        
        TEST("max columns num_columns matches", loaded.num_columns == 32);
        
        for (int i = 0; i < 32; i++) {
            char test_name[64];
            sprintf(test_name, "max column %d name matches", i);
            TEST(test_name, strcmp(loaded.columns[i].name, orig.columns[i].name) == 0);
            
            sprintf(test_name, "max column %d type matches", i);
            TEST(test_name, loaded.columns[i].type == orig.columns[i].type);
        }
    }
    
    // Test 3: schema_save writes to disk and schema_load reads it back correctly
    {
        Schema orig = create_test_schema();
        
        int result = schema_save(&orig, TEST_DIR);
        TEST("schema_save returns 0", result == 0);
        
        Schema loaded;
        memset(&loaded, 0, sizeof(Schema));
        result = schema_load(&loaded, "test_table", TEST_DIR);
        TEST("schema_load returns 0", result == 0);
        
        TEST("loaded table_name matches", strcmp(loaded.table_name, orig.table_name) == 0);
        TEST("loaded num_columns matches", loaded.num_columns == orig.num_columns);
        
        for (int i = 0; i < orig.num_columns; i++) {
            char test_name[64];
            sprintf(test_name, "saved column %d name matches", i);
            TEST(test_name, strcmp(loaded.columns[i].name, orig.columns[i].name) == 0);
            
            sprintf(test_name, "saved column %d type matches", i);
            TEST(test_name, loaded.columns[i].type == orig.columns[i].type);
        }
    }
    
    // Test 4: schema_load on nonexistent file returns -1
    {
        Schema schema;
        memset(&schema, 0, sizeof(Schema));
        int result = schema_load(&schema, "nonexistent_table", TEST_DIR);
        TEST("schema_load on nonexistent file returns -1", result == -1);
    }
    
    // Test 5: schema_serialize with NULL pointer returns -1
    {
        char page_buf[PAGE_SIZE];
        int result = schema_serialize(NULL, page_buf);
        TEST("schema_serialize with NULL schema returns -1", result == -1);
        
        result = schema_serialize(NULL, NULL);
        TEST("schema_serialize with NULL buffer returns -1", result == -1);
    }
    
    // Test 6: schema_deserialize with NULL pointer returns -1
    {
        Schema schema;
        memset(&schema, 0, sizeof(Schema));
        int result = schema_deserialize(NULL, NULL);
        TEST("schema_deserialize with NULL returns -1", result == -1);
    }
    
    // Test 7: schema_get_column_index returns correct index for each column name
    {
        Schema schema = create_test_schema();
        
        int idx = schema_get_column_index(&schema, "id");
        TEST("schema_get_column_index for 'id' returns 0", idx == 0);
        
        idx = schema_get_column_index(&schema, "value");
        TEST("schema_get_column_index for 'value' returns 1", idx == 1);
        
        idx = schema_get_column_index(&schema, "flag");
        TEST("schema_get_column_index for 'flag' returns 2", idx == 2);
        
        idx = schema_get_column_index(&schema, "name");
        TEST("schema_get_column_index for 'name' returns 3", idx == 3);
    }
    
    // Test 8: schema_get_column_index returns -1 for nonexistent column
    {
        Schema schema = create_test_schema();
        
        int idx = schema_get_column_index(&schema, "nonexistent");
        TEST("schema_get_column_index for nonexistent returns -1", idx == -1);
        
        idx = schema_get_column_index(&schema, "id_not_here");
        TEST("schema_get_column_index for 'id_not_here' returns -1", idx == -1);
    }
    
    // Test 9: schema_get_column_index with NULL schema returns -1
    {
        int idx = schema_get_column_index(NULL, "any_column");
        TEST("schema_get_column_index with NULL schema returns -1", idx == -1);
        
        Schema schema;
        memset(&schema, 0, sizeof(Schema));
        idx = schema_get_column_index(&schema, NULL);
        TEST("schema_get_column_index with NULL col_name returns -1", idx == -1);
    }
    
    // Test 10: Empty schema (0 columns)
    {
        Schema empty;
        memset(&empty, 0, sizeof(Schema));
        strcpy(empty.table_name, "empty_table");
        empty.num_columns = 0;
        
        char page_buf[PAGE_SIZE];
        int result = schema_serialize(&empty, page_buf);
        TEST("schema_serialize with 0 columns returns 0", result == 0);
        
        Schema loaded;
        memset(&loaded, 0, sizeof(Schema));
        result = schema_deserialize(&loaded, page_buf);
        TEST("schema_deserialize empty schema returns 0", result == 0);
        TEST("empty schema num_columns is 0", loaded.num_columns == 0);
    }
    
    // Test 11: Row serialize with buffer overflow (too small)
    {
        Schema schema = create_test_schema();
        
        int int_val = 42;
        char varchar_val[100] = "test";
        
        void *values[] = { &int_val, varchar_val };
        int sizes[] = { 4, 4 };
        
        // Buffer de solo 2 bytes (no alcanza para el null bitmap de 1 columna)
        char small_buf[2];
        int out_size;
        int result = row_serialize(&schema, values, sizes, small_buf, 2, &out_size);
        TEST("row_serialize with too small buffer returns -1", result == -1);
    }
    
    // Test 12: Row deserialize with corrupted/truncated buffer
    {
        Schema schema = create_test_schema();
        
        // Buffer truncado (solo tiene el null bitmap pero no los datos)
        char corrupt_buf[1] = {0};
        int sizes_out[4];
        void *values_out[4];
        int dummy_int, dummy_int2;
        char dummy_char;
        char dummy_varchar[50];
        values_out[0] = &dummy_int;
        values_out[1] = &dummy_int2;
        values_out[2] = &dummy_char;
        values_out[3] = dummy_varchar;
        
        int result = row_deserialize(&schema, corrupt_buf, 1, values_out, sizes_out);
        TEST("row_deserialize with truncated buffer returns -1", result == -1);
    }
    
    // Test 13: Row serialize/deserialize with 9+ columns (tests null bitmap > 1 byte)
    {
        Schema schema;
        memset(&schema, 0, sizeof(Schema));
        strcpy(schema.table_name, "many_cols");
        schema.num_columns = 9;  // Requires 2-byte null bitmap
        
        for (int i = 0; i < 9; i++) {
            sprintf(schema.columns[i].name, "col%d", i);
            schema.columns[i].type = TYPE_INT;
            schema.columns[i].max_size = 4;
        }
        
        int vals[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        void *values[] = {&vals[0], &vals[1], &vals[2], &vals[3], &vals[4],
                         &vals[5], &vals[6], &vals[7], &vals[8]};
        int sizes[] = {4, 4, 4, 4, 4, 4, 4, 4, 4};
        
        char buf[PAGE_SIZE];
        int out_size;
        int result = row_serialize(&schema, values, sizes, buf, PAGE_SIZE, &out_size);
        TEST("row_serialize with 9 columns (2-byte bitmap) returns 0", result == 0);
        
        int vals_out[9];
        void *values_out[] = {&vals_out[0], &vals_out[1], &vals_out[2], &vals_out[3], &vals_out[4],
                             &vals_out[5], &vals_out[6], &vals_out[7], &vals_out[8]};
        int sizes_out[9];
        
        result = row_deserialize(&schema, buf, out_size, values_out, sizes_out);
        TEST("row_deserialize with 9 columns returns correct size", result == out_size);
        
        for (int i = 0; i < 9; i++) {
            char test_name[64];
            sprintf(test_name, "9-col row value col%d matches", i);
            TEST(test_name, vals_out[i] == i + 1);
        }
    }
    
    // Test 14: Row with all NULL values
    {
        Schema schema = create_test_schema();
        
        int dummy = 0;
        char dummy_varchar[50];
        void *values[] = { &dummy, dummy_varchar, &dummy, dummy_varchar };
        int sizes[] = { 0, 0, 0, 0 };  // All NULL
        
        char buf[PAGE_SIZE];
        int out_size;
        int result = row_serialize(&schema, values, sizes, buf, PAGE_SIZE, &out_size);
        TEST("row_serialize with all NULLs returns 0", result == 0);
        
        int int_out;
        float float_out;
        char varchar_out[50];
        void *values_out[] = { &int_out, varchar_out, &float_out, varchar_out };
        int sizes_out[4];
        
        result = row_deserialize(&schema, buf, out_size, values_out, sizes_out);
        TEST("row_deserialize with all NULLs returns bytes read", result > 0);
        
        for (int i = 0; i < 4; i++) {
            char test_name[64];
            sprintf(test_name, "all NULL column %d has size 0", i);
            TEST(test_name, sizes_out[i] == 0);
        }
    }
    
    // Test 15: Boolean edge cases (0 and 1)
    {
        Schema schema;
        memset(&schema, 0, sizeof(Schema));
        strcpy(schema.table_name, "bool_test");
        schema.num_columns = 2;
        strcpy(schema.columns[0].name, "flag1");
        schema.columns[0].type = TYPE_BOOLEAN;
        schema.columns[0].max_size = 1;
        strcpy(schema.columns[1].name, "flag2");
        schema.columns[1].type = TYPE_BOOLEAN;
        schema.columns[1].max_size = 1;
        
        char val_true = 1;
        char val_false = 0;
        void *values[] = { &val_true, &val_false };
        int sizes[] = { 1, 1 };
        
        char buf[PAGE_SIZE];
        int out_size;
        int result = row_serialize(&schema, values, sizes, buf, PAGE_SIZE, &out_size);
        TEST("row_serialize with boolean 0 and 1 returns 0", result == 0);
        
        char out_true = 2, out_false = 2;  // init to sentinel values
        void *values_out[] = { &out_true, &out_false };
        int sizes_out[2] = { -1, -1 };
        
        result = row_deserialize(&schema, buf, out_size, values_out, sizes_out);
        TEST("row_deserialize booleans returns correct values",
             out_true == 1 && out_false == 0);
    }
    
    // Test 16: INT edge cases (0, negative, large)
    {
        Schema schema;
        memset(&schema, 0, sizeof(Schema));
        strcpy(schema.table_name, "int_test");
        schema.num_columns = 3;
        strcpy(schema.columns[0].name, "zero");
        schema.columns[0].type = TYPE_INT;
        strcpy(schema.columns[1].name, "neg");
        schema.columns[1].type = TYPE_INT;
        strcpy(schema.columns[2].name, "max");
        schema.columns[2].type = TYPE_INT;
        
        int val_zero = 0;
        int val_neg = -2147483648;  // INT_MIN
        int val_max = 2147483647;  // INT_MAX
        void *values[] = { &val_zero, &val_neg, &val_max };
        int sizes[] = { 4, 4, 4 };
        
        char buf[PAGE_SIZE];
        int out_size;
        int result = row_serialize(&schema, values, sizes, buf, PAGE_SIZE, &out_size);
        TEST("row_serialize with INT edge cases returns 0", result == 0);
        
        int out_zero, out_neg, out_max;
        void *values_out[] = { &out_zero, &out_neg, &out_max };
        int sizes_out[3];
        
        result = row_deserialize(&schema, buf, out_size, values_out, sizes_out);
        TEST("row_deserialize INT zero matches", out_zero == 0);
        TEST("row_deserialize INT_MIN matches", out_neg == -2147483648);
        TEST("row_deserialize INT_MAX matches", out_max == 2147483647);
    }
    
    // Test 17: FLOAT edge cases (0.0, negative, special)
    {
        Schema schema;
        memset(&schema, 0, sizeof(Schema));
        strcpy(schema.table_name, "float_test");
        schema.num_columns = 4;
        strcpy(schema.columns[0].name, "zero");
        schema.columns[0].type = TYPE_FLOAT;
        strcpy(schema.columns[1].name, "neg");
        schema.columns[1].type = TYPE_FLOAT;
        strcpy(schema.columns[2].name, "small");
        schema.columns[2].type = TYPE_FLOAT;
        strcpy(schema.columns[3].name, "large");
        schema.columns[3].type = TYPE_FLOAT;
        
        float val_zero = 0.0f;
        float val_neg = -123.456f;
        float val_small = 0.000001f;
        float val_large = 999999.999f;
        void *values[] = { &val_zero, &val_neg, &val_small, &val_large };
        int sizes[] = { 4, 4, 4, 4 };
        
        char buf[PAGE_SIZE];
        int out_size;
        int result = row_serialize(&schema, values, sizes, buf, PAGE_SIZE, &out_size);
        TEST("row_serialize with FLOAT edge cases returns 0", result == 0);
        
        float out_zero, out_neg, out_small, out_large;
        void *values_out[] = { &out_zero, &out_neg, &out_small, &out_large };
        int sizes_out[4];
        
        result = row_deserialize(&schema, buf, out_size, values_out, sizes_out);
        TEST("row_deserialize FLOAT zero matches", out_zero == 0.0f);
        
        float diff_neg = out_neg - (-123.456f);
        if (diff_neg < 0) diff_neg = -diff_neg;
        TEST("row_deserialize negative float matches", diff_neg < 0.001f);
        
        float diff_small = out_small - 0.000001f;
        if (diff_small < 0) diff_small = -diff_small;
        TEST("row_deserialize small float matches", diff_small < 0.0000001f);
        
        float diff_large = out_large - 999999.999f;
        if (diff_large < 0) diff_large = -diff_large;
        TEST("row_deserialize large float matches", diff_large < 1.0f);
    }
    
    // Test 18: VARCHAR max size (255 bytes)
    {
        Schema schema;
        memset(&schema, 0, sizeof(Schema));
        strcpy(schema.table_name, "varchar_max");
        schema.num_columns = 1;
        strcpy(schema.columns[0].name, "data");
        schema.columns[0].type = TYPE_VARCHAR;
        schema.columns[0].max_size = 255;
        
        char val[256];
        memset(val, 'X', 255);
        val[255] = '\0';
        
        void *values[] = { val };
        int sizes[] = { 255 };
        
        char buf[PAGE_SIZE];
        int out_size;
        int result = row_serialize(&schema, values, sizes, buf, PAGE_SIZE, &out_size);
        TEST("row_serialize with 255-byte VARCHAR returns 0", result == 0);
        TEST("row_serialize output size includes length prefix", out_size > 255);
        
        char out[256];
        void *values_out[] = { out };
        int sizes_out[1];
        
        result = row_deserialize(&schema, buf, out_size, values_out, sizes_out);
        TEST("row_deserialize 255-byte VARCHAR returns correct size", sizes_out[0] == 255);
        
        // Check all bytes are 'X'
        int all_match = 1;
        for (int i = 0; i < 255; i++) {
            if (out[i] != 'X') {
                all_match = 0;
                break;
            }
        }
        TEST("row_deserialize 255-byte VARCHAR data matches", all_match);
    }
    
    // Test 19: schema_deserialize with invalid num_columns (> MAX_COLUMNS)
    {
        char corrupt_buf[PAGE_SIZE];
        memset(corrupt_buf, 0, PAGE_SIZE);
        
        // Write table_name
        strcpy(corrupt_buf, "test");
        
        // Write num_columns > MAX_COLUMNS
        int bad_num = 100;
        memcpy(corrupt_buf + MAX_TABLE_NAME, &bad_num, sizeof(int));
        
        Schema schema;
        memset(&schema, 0, sizeof(Schema));
        int result = schema_deserialize(&schema, corrupt_buf);
        TEST("schema_deserialize with invalid num_columns returns -1", result == -1);
    }
    
    // Test 20: schema_save and schema_load with NULL pointers
    {
        Schema schema;
        memset(&schema, 0, sizeof(Schema));
        strcpy(schema.table_name, "null_test");
        schema.num_columns = 1;
        strcpy(schema.columns[0].name, "col1");
        schema.columns[0].type = TYPE_INT;
        
        int result = schema_save(NULL, TEST_DIR);
        TEST("schema_save with NULL schema returns -1", result == -1);
        
        result = schema_save(&schema, NULL);
        TEST("schema_save with NULL data_dir returns -1", result == -1);
        
        memset(&schema, 0, sizeof(Schema));
        result = schema_load(&schema, "test", NULL);
        TEST("schema_load with NULL data_dir returns -1", result == -1);
        
        result = schema_load(&schema, NULL, TEST_DIR);
        TEST("schema_load with NULL table_name returns -1", result == -1);
    }
    
    // Cleanup
    system("rm -rf " TEST_DIR);
    
    printf("\n=== Summary ===\n");
    printf("%d/%d tests passed\n", tests_passed, tests_total);
    
    return (tests_passed == tests_total) ? 0 : 1;
}