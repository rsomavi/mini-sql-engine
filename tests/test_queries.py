#!/usr/bin/env python3
"""
Test suite for the SQL engine.
Verifies all functionality works correctly after refactoring.
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parser import get_parser
from planner import QueryPlanner
from executor import QueryExecutor
from storage import MemoryStorage


# ============================================================================
# Test Database Setup
# ============================================================================

def create_test_database():
    """Create the test database with sample data."""
    return {
        "users": [
            {"id": 1, "name": "Juan", "age": 25, "city": "Madrid"},
            {"id": 2, "name": "Ana", "age": 30, "city": "Barcelona"},
            {"id": 3, "name": "Luis", "age": 22, "city": "Valencia"},
            {"id": 4, "name": "Maria", "age": 28, "city": "Sevilla"},
            {"id": 5, "name": "Carlos", "age": 35, "city": "Madrid"},
            {"id": 6, "name": "Laura", "age": 27, "city": "Bilbao"},
            {"id": 7, "name": "Pedro", "age": 40, "city": "Valencia"},
            {"id": 8, "name": "Sofia", "age": 19, "city": "Barcelona"},
            {"id": 9, "name": "Miguel", "age": 31, "city": "Sevilla"},
            {"id": 10, "name": "Elena", "age": 24, "city": "Madrid"},
            {"id": 11, "name": "Diego", "age": 29, "city": "Bilbao"},
            {"id": 12, "name": "Lucia", "age": 21, "city": "Valencia"},
            {"id": 13, "name": "Alberto", "age": 45, "city": "Madrid"},
            {"id": 14, "name": "Paula", "age": 26, "city": "Barcelona"},
            {"id": 15, "name": "Jorge", "age": 33, "city": "Sevilla"}
        ],
        "products": [
            {"id": 1, "name": "Laptop", "price": 1200},
            {"id": 2, "name": "Phone", "price": 800},
            {"id": 3, "name": "Keyboard", "price": 100},
            {"id": 4, "name": "Mouse", "price": 40},
            {"id": 5, "name": "Monitor", "price": 300},
            {"id": 6, "name": "Tablet", "price": 500},
            {"id": 7, "name": "Headphones", "price": 150},
            {"id": 8, "name": "Speaker", "price": 200},
            {"id": 9, "name": "Webcam", "price": 90},
            {"id": 10, "name": "Printer", "price": 250}
        ],
        "orders": [
            {"id": 1, "user_id": 1, "product": "Laptop", "amount": 1200},
            {"id": 2, "user_id": 2, "product": "Phone", "amount": 800},
            {"id": 3, "user_id": 3, "product": "Keyboard", "amount": 100},
            {"id": 4, "user_id": 1, "product": "Mouse", "amount": 40},
            {"id": 5, "user_id": 4, "product": "Monitor", "amount": 300},
            {"id": 6, "user_id": 5, "product": "Tablet", "amount": 500},
            {"id": 7, "user_id": 6, "product": "Headphones", "amount": 150},
            {"id": 8, "user_id": 7, "product": "Speaker", "amount": 200},
            {"id": 9, "user_id": 8, "product": "Webcam", "amount": 90},
            {"id": 10, "user_id": 9, "product": "Printer", "amount": 250}
        ]
    }


# ============================================================================
# Test Runner
# ============================================================================

def run_query(executor, query):
    """Execute a query and return the result or None if it fails."""
    try:
        parser = get_parser()
        planner = QueryPlanner()
        ast = parser.parse(query)
        if ast is None:
            return {"error": "Parse error"}
        plan = planner.plan(ast)
        result = executor.execute(plan)
        return {"result": result}
    except Exception as e:
        return {"error": str(e)}


def run_test(executor, query, expected, check_type="length"):
    """
    Run a single test case.
    
    Args:
        executor: QueryExecutor instance
        query: SQL query string
        expected: Expected result
        check_type: How to check result ("length", "value", "error", "contains")
    
    Returns:
        tuple: (passed: bool, message: str)
    """
    result = run_query(executor, query)
    
    if "error" in result:
        if check_type == "error":
            return True, f"PASS (expected error): {query}"
        return False, f"FAIL: {query} - Error: {result['error']}"
    
    actual = result["result"]
    
    if check_type == "length":
        if isinstance(actual, list):
            actual_result = len(actual)
        else:
            actual_result = actual
        if actual_result == expected:
            return True, f"PASS: {query} -> {actual_result}"
        return False, f"FAIL: {query} - Expected {expected}, got {actual_result}"
    
    elif check_type == "value":
        if actual == expected:
            return True, f"PASS: {query} -> {actual}"
        return False, f"FAIL: {query} - Expected {expected}, got {actual}"
    
    elif check_type == "contains":
        if expected in actual:
            return True, f"PASS: {query} contains {expected}"
        return False, f"FAIL: {query} - Expected to contain {expected}, got {actual}"
    
    elif check_type == "error":
        return False, f"FAIL: {query} - Expected error but got result: {actual}"
    
    return False, f"FAIL: {query} - Unknown check_type"


def run_all_tests():
    """Run all test cases and report results."""
    # Setup
    database = create_test_database()
    storage = MemoryStorage(database)
    executor = QueryExecutor(storage)
    
    # Track results
    passed = 0
    failed = 0
    results = []
    
    print("=" * 70)
    print("SQL Engine Test Suite")
    print("=" * 70)
    print()
    
    # ========================================================================
    # BASIC SELECT
    # ========================================================================
    print("--- BASIC SELECT ---")
    
    tests = [
        # SELECT * FROM table
        ("SELECT * FROM users", 15, "length"),
        ("SELECT * FROM products", 10, "length"),
        ("SELECT * FROM orders", 10, "length"),
        
        # SELECT single column
        ("SELECT name FROM users", 15, "length"),
        ("SELECT age FROM users", 15, "length"),
        ("SELECT city FROM users", 15, "length"),
        
        # SELECT multiple columns
        ("SELECT name, age FROM users", 15, "length"),
        ("SELECT id, name FROM users", 15, "length"),
        ("SELECT name, city FROM users", 15, "length"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # WHERE CONDITIONS
    # ========================================================================
    print("--- WHERE CONDITIONS ---")
    
    tests = [
        # Greater than
        ("SELECT name FROM users WHERE age > 30", 5, "length"),
        ("SELECT name FROM users WHERE age > 40", 1, "length"),
        ("SELECT name FROM users WHERE age > 50", 0, "length"),
        
        # Greater than or equal
        ("SELECT name FROM users WHERE age >= 30", 6, "length"),
        ("SELECT name FROM users WHERE age >= 35", 3, "length"),
        
        # Less than
        ("SELECT name FROM users WHERE age < 25", 4, "length"),
        ("SELECT name FROM users WHERE age < 20", 1, "length"),
        
        # Less than or equal
        ("SELECT name FROM users WHERE age <= 22", 3, "length"),
        
        # Equality
        ("SELECT name FROM users WHERE city = \"Madrid\"", 4, "length"),
        ("SELECT name FROM users WHERE city = \"Barcelona\"", 3, "length"),
        ("SELECT name FROM users WHERE city = \"FakeCity\"", 0, "length"),
        
        # Not equal (using NOT)
        ("SELECT name FROM users WHERE NOT city = \"Madrid\"", 11, "length"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # LOGICAL OPERATORS
    # ========================================================================
    print("--- LOGICAL OPERATORS ---")
    
    tests = [
        # AND
        ("SELECT name FROM users WHERE age > 25 AND age < 35", 7, "length"),
        ("SELECT name FROM users WHERE age > 20 AND city = \"Madrid\"", 4, "length"),
        
        # OR
        ("SELECT name FROM users WHERE city = \"Madrid\" OR city = \"Barcelona\"", 7, "length"),
        ("SELECT name FROM users WHERE city = \"Madrid\" OR city = \"Fake\"", 4, "length"),
        
        # NOT
        ("SELECT name FROM users WHERE NOT age > 30", 10, "length"),
        ("SELECT name FROM users WHERE NOT city = \"Madrid\"", 11, "length"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # DISTINCT
    # ========================================================================
    print("--- DISTINCT ---")
    
    tests = [
        ("SELECT DISTINCT city FROM users", 5, "length"),
        ("SELECT DISTINCT age FROM users", 15, "length"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # ORDER BY
    # ========================================================================
    print("--- ORDER BY ---")
    
    tests = [
        ("SELECT name, age FROM users ORDER BY age", 15, "length"),
        ("SELECT name FROM users ORDER BY age LIMIT 1", 1, "length"),
        ("SELECT name, age FROM users ORDER BY age LIMIT 3", 3, "length"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # LIMIT
    # ========================================================================
    print("--- LIMIT ---")
    
    tests = [
        ("SELECT name FROM users LIMIT 5", 5, "length"),
        ("SELECT name FROM users LIMIT 10", 10, "length"),
        ("SELECT name FROM users LIMIT 100", 15, "length"),
        # LIMIT 0 returns all rows (no special handling in this engine)
        ("SELECT name FROM users LIMIT 0", 15, "length"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # AGGREGATIONS
    # ========================================================================
    print("--- AGGREGATIONS ---")
    
    tests = [
        # COUNT
        ("SELECT COUNT(*) FROM users", 15, "value"),
        ("SELECT COUNT(*) FROM products", 10, "value"),
        ("SELECT COUNT(*) FROM orders", 10, "value"),
        
        # SUM
        ("SELECT SUM(price) FROM products", 3630, "value"),
        ("SELECT SUM(age) FROM users", 435, "value"),
        ("SELECT SUM(amount) FROM orders", 3630, "value"),
        
        # AVG
        ("SELECT AVG(age) FROM users", 29.0, "value"),
        ("SELECT AVG(price) FROM products", 363.0, "value"),
        
        # MIN
        ("SELECT MIN(price) FROM products", 40, "value"),
        ("SELECT MIN(age) FROM users", 19, "value"),
        
        # MAX
        ("SELECT MAX(price) FROM products", 1200, "value"),
        ("SELECT MAX(age) FROM users", 45, "value"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # AGGREGATIONS WITH WHERE
    # ========================================================================
    print("--- AGGREGATIONS WITH WHERE ---")
    
    tests = [
        # SUM with WHERE
        ("SELECT SUM(price) FROM products WHERE price > 500", 2000, "value"),
        ("SELECT SUM(price) FROM products WHERE price > 1000", 1200, "value"),
        ("SELECT SUM(price) FROM products WHERE price > 10000", 0, "value"),
        
        # AVG with WHERE
        ("SELECT AVG(age) FROM users WHERE city = \"Madrid\"", 32.25, "value"),
        ("SELECT AVG(age) FROM users WHERE age > 30", 36.8, "value"),
        
        # MIN with WHERE
        ("SELECT MIN(age) FROM users WHERE age > 25", 26, "value"),
        ("SELECT MIN(age) FROM users WHERE city = \"Madrid\"", 24, "value"),
        ("SELECT MIN(price) FROM products WHERE price > 500", 800, "value"),
        
        # MAX with WHERE
        ("SELECT MAX(age) FROM users WHERE age < 40", 35, "value"),
        ("SELECT MAX(age) FROM users WHERE city = \"Madrid\"", 45, "value"),
        ("SELECT MAX(price) FROM products WHERE price < 500", 300, "value"),
        
        # COUNT with WHERE
        ("SELECT COUNT(*) FROM users WHERE age > 30", 5, "value"),
        ("SELECT COUNT(*) FROM users WHERE city = \"Madrid\"", 4, "value"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # EDGE CASES
    # ========================================================================
    print("--- EDGE CASES ---")
    
    tests = [
        # Empty results
        ("SELECT SUM(price) FROM products WHERE price > 10000", 0, "value"),
        ("SELECT AVG(age) FROM users WHERE age > 100", 0, "value"),
        ("SELECT MIN(price) FROM products WHERE price > 99999", 0, "value"),
        ("SELECT MAX(price) FROM products WHERE price < 1", 0, "value"),
        ("SELECT COUNT(*) FROM users WHERE age > 100", 0, "value"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # ERROR TESTS
    # ========================================================================
    print("--- ERROR TESTS ---")
    
    tests = [
        # Invalid column
        ("SELECT fake_column FROM users", None, "error"),
        ("SELECT name, fake_column FROM users", None, "error"),
        
        # Invalid table
        ("SELECT name FROM fake_table", None, "error"),
        
        # Invalid aggregation column
        ("SELECT SUM(fake_column) FROM products", None, "error"),
        ("SELECT AVG(fake_column) FROM users", None, "error"),
        ("SELECT MIN(fake_column) FROM products", None, "error"),
        ("SELECT MAX(fake_column) FROM products", None, "error"),
        
        # Invalid ORDER BY column
        ("SELECT name FROM users ORDER BY fake_column", None, "error"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # COMPLEX QUERIES
    # ========================================================================
    print("--- COMPLEX QUERIES ---")
    
    tests = [
        # DISTINCT + ORDER BY
        ("SELECT DISTINCT city FROM users ORDER BY city", 5, "length"),
        
        # WHERE + ORDER BY + LIMIT
        ("SELECT name FROM users WHERE age > 20 AND city = \"Madrid\" ORDER BY age LIMIT 2", 2, "length"),
        
        # Multiple conditions
        ("SELECT name, age FROM users WHERE age >= 20 AND age <= 30 AND city = \"Madrid\"", 2, "length"),
        
        # DISTINCT + LIMIT
        ("SELECT DISTINCT city FROM users LIMIT 3", 3, "length"),
    ]
    
    for query, expected, check_type in tests:
        ok, msg = run_test(executor, query, expected, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed, {passed + failed} total")
    print("=" * 70)
    
    return passed, failed


if __name__ == "__main__":
    run_all_tests()
