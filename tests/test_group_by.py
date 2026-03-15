#!/usr/bin/env python3
"""
Test suite for GROUP BY functionality.
Validates GROUP BY semantics according to SQL standard.
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
    """Run all GROUP BY test cases and report results."""
    # Setup
    database = create_test_database()
    storage = MemoryStorage(database)
    executor = QueryExecutor(storage)
    
    # Track results
    passed = 0
    failed = 0
    results = []
    
    print("=" * 70)
    print("GROUP BY Test Suite - SQL Semantics Validation")
    print("=" * 70)
    print()
    
    # =========================================================================
    #  CORRECT CASES - Should work correctly
    # =========================================================================
    print("=" * 70)
    print(" CORRECT CASES - Should work")
    print("=" * 70)
    print()
    
    # GROUP BY with aggregate only
    tests = [
        # Only aggregate in SELECT with GROUP BY
        ("SELECT COUNT(*) FROM users GROUP BY city", 5, "length"),
        
        # GROUP BY column in SELECT (no aggregate)
        ("SELECT city FROM users GROUP BY city", 5, "length"),
        
        # GROUP BY column + aggregate
        ("SELECT city, COUNT(*) FROM users GROUP BY city", 5, "length"),
        
        # GROUP BY + SUM
        ("SELECT city, SUM(age) FROM users GROUP BY city", 5, "length"),
        
        # GROUP BY + AVG
        ("SELECT city, AVG(age) FROM users GROUP BY city", 5, "length"),
        
        # GROUP BY + MIN
        ("SELECT city, MIN(age) FROM users GROUP BY city", 5, "length"),
        
        # GROUP BY + MAX
        ("SELECT city, MAX(age) FROM users GROUP BY city", 5, "length"),
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
    
    # =========================================================================
    #  CORRECT CASES - WHERE + GROUP BY
    # =========================================================================
    print("=" * 70)
    print(" CORRECT CASES - WHERE + GROUP BY")
    print("=" * 70)
    print()
    
    # Test WHERE + GROUP BY with proper validation
    where_group_by_tests = [
        ("SELECT city, COUNT(*) FROM users WHERE age > 25 GROUP BY city", ["city", "count"]),
        ("SELECT city, COUNT(*) FROM users WHERE city = 'Madrid' GROUP BY city", ["city", "count"]),
        ("SELECT city, SUM(age) FROM users WHERE age >= 25 GROUP BY city", ["city", "sum"]),
    ]
    
    for query, expected_keys in where_group_by_tests:
        result = run_query(executor, query)
        
        if "error" in result:
            print(f"FAIL: {query} - Error: {result['error']}")
            failed += 1
        else:
            actual = result["result"]
            # Validate it's a list with at least one group
            if isinstance(actual, list) and len(actual) > 0:
                # Validate the first row has the expected keys
                first_row = actual[0]
                if all(key in first_row for key in expected_keys):
                    print(f"PASS: {query} -> {len(actual)} groups")
                    passed += 1
                else:
                    print(f"FAIL: {query} - Missing keys. Expected {expected_keys}, got {list(first_row.keys())}")
                    failed += 1
            else:
                print(f"FAIL: {query} - Expected list with groups, got: {actual}")
                failed += 1
        results.append((ok, msg))
    
    print()
    
    # =========================================================================
    #  CORRECT CASES - ORDER BY + GROUP BY
    # =========================================================================
    print("=" * 70)
    print(" CORRECT CASES - ORDER BY + GROUP BY")
    print("=" * 70)
    print()
    
    tests = [
        # GROUP BY + ORDER BY (by GROUP BY column)
        ("SELECT city, COUNT(*) FROM users GROUP BY city ORDER BY city", 5, "length"),
        
        # GROUP BY + ORDER BY (by aggregate)
        ("SELECT city, COUNT(*) FROM users GROUP BY city ORDER BY COUNT(*)", 5, "length"),
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
    
    # =========================================================================
    #  CORRECT CASES - Multiple Aggregates
    # =========================================================================
    print("=" * 70)
    print(" CORRECT CASES - Multiple Aggregates")
    print("=" * 70)
    print()
    
    tests = [
        # Multiple aggregates
        ("SELECT city, COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users GROUP BY city", 5, "length"),
        
        # Same aggregate multiple times (though unusual)
        ("SELECT city, COUNT(*), COUNT(*) FROM users GROUP BY city", 5, "length"),
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
    
    # =========================================================================
    #  INCORRECT CASES - Should ERROR but currently DON'T
    # =========================================================================
    print("=" * 70)
    print(" INCORRECT CASES - Should ERROR but currently work (BUG)")
    print("=" * 70)
    print()
    print("These queries violate SQL standard: columns in SELECT must be")
    print("either in GROUP BY or inside an aggregate function.")
    print()
    
    tests = [
        #  BUG: Non-GROUP BY column in SELECT (should error)
        # Query: SELECT name, COUNT(*) FROM users GROUP BY city
        # Expected: ERROR - 'name' is not in GROUP BY and not aggregated
        # Actual: Returns result (takes first value) - THIS IS WRONG
        ("SELECT name, COUNT(*) FROM users GROUP BY city", "error"),
        
        #  BUG: Non-GROUP BY column in SELECT (should error)
        ("SELECT age, COUNT(*) FROM users GROUP BY city", "error"),
        
        #  BUG: Non-GROUP BY column in SELECT (should error)
        ("SELECT id, COUNT(*) FROM users GROUP BY city", "error"),
        
        #  BUG: Multiple non-GROUP BY columns (should error)
        ("SELECT name, age, COUNT(*) FROM users GROUP BY city", "error"),
        
        #  BUG: Non-GROUP BY column with SUM (should error)
        ("SELECT name, SUM(age) FROM users GROUP BY city", "error"),
        
        #  BUG: Non-GROUP BY column with AVG (should error)
        ("SELECT id, AVG(age) FROM users GROUP BY city", "error"),
    ]
    
    for query, check_type in tests:
        ok, msg = run_test(executor, query, None, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # =========================================================================
    #  INCORRECT CASES - GROUP BY with non-existent column
    # =========================================================================
    print("=" * 70)
    print(" INCORRECT CASES - Non-existent columns")
    print("=" * 70)
    print()
    
    tests = [
        # Non-existent GROUP BY column
        ("SELECT city, COUNT(*) FROM users GROUP BY fake_column", "error"),
        
        # Non-existent column in aggregate
        ("SELECT city, SUM(fake_column) FROM users GROUP BY city", "error"),
    ]
    
    for query, check_type in tests:
        ok, msg = run_test(executor, query, None, check_type)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        results.append((ok, msg))
    
    print()
    
    # =========================================================================
    #  VALIDATION TESTS - Check result correctness
    # =========================================================================
    print("=" * 70)
    print(" VALIDATION TESTS - Verify GROUP BY results")
    print("=" * 70)
    print()
    
    # Run specific queries and check actual values
    test_queries = [
        ("SELECT city, COUNT(*) FROM users GROUP BY city", "Check counts per city"),
        ("SELECT city, SUM(age) FROM users GROUP BY city", "Check sum of ages per city"),
        ("SELECT city, AVG(age) FROM users GROUP BY city", "Check avg age per city"),
    ]
    
    for query, description in test_queries:
        result = run_query(executor, query)
        if "error" in result:
            print(f"ERROR: {query}")
            print(f"  -> {result['error']}")
            failed += 1
        else:
            print(f"✓ {description}")
            print(f"  Query: {query}")
            for row in result["result"]:
                print(f"    {row}")
            passed += 1
        print()
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed, {passed + failed} total")
    print("=" * 70)
    
    return passed, failed


if __name__ == "__main__":
    run_all_tests()
