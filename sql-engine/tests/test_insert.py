#!/usr/bin/env python3
# test_insert.py — Test completo de INSERT para miniDBMS
# Requiere: servidor corriendo en localhost:5433
# Ejecutar desde sql-parse/sql-engine/:
#   python3 tests/test_insert.py

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from storage_server import ServerStorage
from parser   import get_parser
from planner  import QueryPlanner
from executor import QueryExecutor

# ============================================================================
# Infraestructura
# ============================================================================

tests_passed = 0
tests_failed  = 0

def check(condition, msg):
    global tests_passed, tests_failed
    if condition:
        print(f"  PASS: {msg}")
        tests_passed += 1
    else:
        print(f"  FAIL: {msg}")
        tests_failed += 1

def make_stack():
    storage  = ServerStorage()
    executor = QueryExecutor(storage)
    planner  = QueryPlanner()
    parser   = get_parser()
    return storage, executor, planner, parser

def sql(executor, planner, parser, query):
    """Ejecuta una query SQL y devuelve los resultados."""
    ast  = parser.parse(query)
    plan = planner.plan(ast)
    return executor.execute(plan)

TABLE = "test_insert_tmp"

def setup():
    """Crea la tabla de test. Si ya existe la ignora."""
    storage, executor, planner, parser = make_stack()
    try:
        sql(executor, planner, parser,
            f"CREATE TABLE {TABLE} "
            f"(id INT, nombre VARCHAR(50), salario INT, activo INT)")
    except Exception:
        pass  # ya existe, OK

def teardown():
    """No podemos hacer DROP TABLE todavía — lo dejamos para cuando haya DELETE."""
    pass

# ============================================================================
# Bloque 1: ROW_ID básico
# ============================================================================

def test_insert_returns_row_id():
    storage, executor, planner, parser = make_stack()
    result = sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (1, 'Ana', 35000, 1)")
    check(len(result) == 1, "insert devuelve exactamente 1 fila de resultado")
    check("row_id" in result[0], "resultado contiene campo row_id")

def test_row_id_is_int():
    storage, executor, planner, parser = make_stack()
    result = sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (2, 'Bob', 28000, 1)")
    row_id = result[0]["row_id"]
    check(isinstance(row_id, int), f"row_id={row_id} es int")

def test_row_id_encodes_page_and_slot():
    """ROW_ID = (page_id << 16) | slot_id — page_id >= 1 siempre."""
    storage, executor, planner, parser = make_stack()
    result = sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (3, 'Carlos', 42000, 0)")
    row_id   = result[0]["row_id"]
    page_id  = row_id >> 16
    slot_id  = row_id & 0xFFFF
    check(page_id >= 1,  f"page_id={page_id} >= 1 (página 0 es schema)")
    check(slot_id >= 0,  f"slot_id={slot_id} >= 0")

def test_consecutive_inserts_different_row_ids():
    storage, executor, planner, parser = make_stack()
    r1 = sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (4, 'Diana', 51000, 1)")
    r2 = sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (5, 'Eva', 33000, 1)")
    check(r1[0]["row_id"] != r2[0]["row_id"],
          f"inserts consecutivos tienen row_ids distintos: {r1[0]['row_id']} != {r2[0]['row_id']}")

# ============================================================================
# Bloque 2: INSERT sin columnas (nueva funcionalidad)
# ============================================================================

def test_insert_no_columns_returns_row_id():
    storage, executor, planner, parser = make_stack()
    result = sql(executor, planner, parser,
        f"INSERT INTO {TABLE} VALUES (6, 'Felipe', 29000, 1)")
    check("row_id" in result[0], "INSERT sin columnas devuelve row_id")

def test_insert_no_columns_data_readable():
    """Inserta sin columnas y verifica que SELECT lo lee correctamente."""
    storage, executor, planner, parser = make_stack()
    sql(executor, planner, parser,
        f"INSERT INTO {TABLE} VALUES (100, 'Ghost', 99000, 1)")
    rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
    ids = [r["id"] for r in rows]
    check(100 in ids, "fila insertada sin columnas aparece en SELECT (id=100)")

def test_insert_no_columns_values_correct():
    """Verifica que los valores insertados sin columnas son correctos."""
    storage, executor, planner, parser = make_stack()
    sql(executor, planner, parser,
        f"INSERT INTO {TABLE} VALUES (101, 'Verificado', 77777, 0)")
    rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
    target = [r for r in rows if r["id"] == 101]
    check(len(target) == 1, "exactamente 1 fila con id=101")
    if target:
        r = target[0]
        check(r["nombre"].strip() == "Verificado", f"nombre correcto: {r['nombre']!r}")
        check(r["salario"] == 77777, f"salario correcto: {r['salario']}")
        check(r["activo"] == 0, f"activo correcto: {r['activo']}")

# ============================================================================
# Bloque 3: Round-trip — INSERT + SELECT verifica datos
# ============================================================================

def test_roundtrip_int():
    storage, executor, planner, parser = make_stack()
    sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (200, 'RT_Int', 12345, 1)")
    rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
    target = [r for r in rows if r["id"] == 200]
    check(len(target) >= 1, "fila round-trip int encontrada")
    if target:
        check(target[0]["salario"] == 12345, f"INT 12345 round-trip OK: {target[0]['salario']}")

def test_roundtrip_varchar():
    storage, executor, planner, parser = make_stack()
    sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (201, 'Gonzalez', 0, 1)")
    rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
    target = [r for r in rows if r["id"] == 201]
    check(len(target) >= 1, "fila round-trip varchar encontrada")
    if target:
        check(target[0]["nombre"].strip() == "Gonzalez",
              f"VARCHAR round-trip OK: {target[0]['nombre']!r}")

def test_roundtrip_zero_values():
    """Valores a cero no deben confundirse con NULL."""
    storage, executor, planner, parser = make_stack()
    sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (202, 'Zero', 0, 0)")
    rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
    target = [r for r in rows if r["id"] == 202]
    check(len(target) >= 1, "fila con valores cero encontrada")
    if target:
        check(target[0]["salario"] == 0,  "salario=0 round-trip OK")
        check(target[0]["activo"]  == 0,  "activo=0 round-trip OK")

def test_roundtrip_negative_int():
    storage, executor, planner, parser = make_stack()
    sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (203, 'Negativo', -5000, 1)")
    rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
    target = [r for r in rows if r["id"] == 203]
    check(len(target) >= 1, "fila con INT negativo encontrada")
    if target:
        check(target[0]["salario"] == -5000,
              f"INT negativo round-trip OK: {target[0]['salario']}")

def test_roundtrip_max_int():
    """INT32 máximo: 2147483647."""
    storage, executor, planner, parser = make_stack()
    sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (204, 'MaxInt', 2147483647, 1)")
    rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
    target = [r for r in rows if r["id"] == 204]
    check(len(target) >= 1, "fila con INT32 max encontrada")
    if target:
        check(target[0]["salario"] == 2147483647,
              f"INT32 max round-trip OK: {target[0]['salario']}")

def test_roundtrip_varchar_long():
    """VARCHAR cerca del límite (50 chars)."""
    storage, executor, planner, parser = make_stack()
    nombre_largo = "A" * 48  # 48 chars, límite es 50
    sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (205, '{nombre_largo}', 1, 1)")
    rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
    target = [r for r in rows if r["id"] == 205]
    check(len(target) >= 1, "fila con VARCHAR largo encontrada")
    if target:
        check(target[0]["nombre"].strip() == nombre_largo,
              f"VARCHAR 48 chars round-trip OK (len={len(target[0]['nombre'].strip())})")

def test_roundtrip_varchar_single_char():
    storage, executor, planner, parser = make_stack()
    sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (206, 'X', 1, 1)")
    rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
    target = [r for r in rows if r["id"] == 206]
    check(len(target) >= 1, "fila con VARCHAR 1 char encontrada")
    if target:
        check(target[0]["nombre"].strip() == "X", "VARCHAR 1 char round-trip OK")

# ============================================================================
# Bloque 4: múltiples inserts — integridad
# ============================================================================

def test_multiple_inserts_all_readable():
    """Inserta 10 filas y verifica que todas aparecen en SELECT."""
    storage, executor, planner, parser = make_stack()
    ids_insertados = list(range(300, 310))
    for i in ids_insertados:
        sql(executor, planner, parser,
            f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES ({i}, 'Bulk{i}', {i*100}, 1)")
    rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
    ids_leidos = {r["id"] for r in rows}
    encontrados = [i for i in ids_insertados if i in ids_leidos]
    check(len(encontrados) == len(ids_insertados),
          f"bulk insert: {len(encontrados)}/{len(ids_insertados)} filas encontradas")

def test_insert_order_preserved_in_rowid():
    """Los ROW_IDs deben ser crecientes dentro de la misma página."""
    storage, executor, planner, parser = make_stack()
    r1 = sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (400, 'Primero', 1, 1)")
    r2 = sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (401, 'Segundo', 2, 1)")
    rid1 = r1[0]["row_id"]
    rid2 = r2[0]["row_id"]
    # Si están en la misma página, slot_id debe crecer
    if (rid1 >> 16) == (rid2 >> 16):
        slot1 = rid1 & 0xFFFF
        slot2 = rid2 & 0xFFFF
        check(slot2 > slot1,
              f"slots crecientes en misma página: slot {slot1} -> {slot2}")
    else:
        check(True, "filas en páginas distintas — orden no aplica")

# ============================================================================
# Bloque 5: errores esperados
# ============================================================================

def test_insert_table_not_found():
    """INSERT en tabla inexistente debe lanzar RuntimeError."""
    storage, executor, planner, parser = make_stack()
    try:
        sql(executor, planner, parser,
            "INSERT INTO tabla_que_no_existe (id) VALUES (1)")
        check(False, "INSERT en tabla inexistente debería lanzar excepción")
    except (RuntimeError, Exception) as e:
        check(True, f"INSERT en tabla inexistente lanza excepción: {type(e).__name__}")

def test_insert_result_message():
    """El mensaje de resultado debe ser '1 row inserted'."""
    storage, executor, planner, parser = make_stack()
    result = sql(executor, planner, parser,
        f"INSERT INTO {TABLE} (id, nombre, salario, activo) VALUES (500, 'Msg', 1, 1)")
    check(result[0].get("result") == "1 row inserted",
          f"mensaje correcto: {result[0].get('result')!r}")

# ============================================================================
# Bloque 6: persistencia — reinicio del servidor
# (Solo comprueba que los datos insertados en bloques anteriores siguen ahí)
# ============================================================================

def test_persistence_previous_data_survives():
    """
    Si el servidor fue reiniciado entre sesiones, los datos deben seguir ahí.
    Este test verifica que la tabla de test existe y tiene filas.
    """
    storage, executor, planner, parser = make_stack()
    try:
        rows = sql(executor, planner, parser, f"SELECT * FROM {TABLE}")
        check(len(rows) > 0,
              f"datos persisten tras reinicio del servidor ({len(rows)} filas)")
    except Exception as e:
        check(False, f"no se pudo leer tabla tras reinicio: {e}")

# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=== TEST INSERT — miniDBMS ===\n")
    print(f"Tabla de test: {TABLE}")
    print("NOTA: requiere servidor corriendo en localhost:5433\n")

    setup()

    print("-- Bloque 1: ROW_ID básico --")
    test_insert_returns_row_id()
    test_row_id_is_int()
    test_row_id_encodes_page_and_slot()
    test_consecutive_inserts_different_row_ids()

    print("\n-- Bloque 2: INSERT sin columnas --")
    test_insert_no_columns_returns_row_id()
    test_insert_no_columns_data_readable()
    test_insert_no_columns_values_correct()

    print("\n-- Bloque 3: Round-trip INSERT + SELECT --")
    test_roundtrip_int()
    test_roundtrip_varchar()
    test_roundtrip_zero_values()
    test_roundtrip_negative_int()
    test_roundtrip_max_int()
    test_roundtrip_varchar_long()
    test_roundtrip_varchar_single_char()

    print("\n-- Bloque 4: múltiples inserts --")
    test_multiple_inserts_all_readable()
    test_insert_order_preserved_in_rowid()

    print("\n-- Bloque 5: errores esperados --")
    test_insert_table_not_found()
    test_insert_result_message()

    print("\n-- Bloque 6: persistencia --")
    test_persistence_previous_data_survives()

    print(f"\n=== RESULTADO: {tests_passed} passed, {tests_failed} failed, "
          f"{tests_passed + tests_failed} total ===")
