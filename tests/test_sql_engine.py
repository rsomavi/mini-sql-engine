#!/usr/bin/env python3
"""
Test suite for SQL engine fixes verification.
Verifies that all reported issues have been resolved.
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
        ]
    }


def run_query(executor, query):
    """Execute a query and return the result or error."""
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


# ============================================================================
# Test Cases
# ============================================================================

def test_count_with_group_by(executor):
    """Test: SELECT COUNT(*) FROM users GROUP BY city"""
    query = "SELECT COUNT(*) FROM users GROUP BY city"
    result = run_query(executor, query)
    
    if "error" in result:
        return False, f"FAIL: {query} - Error: {result['error']}"
    
    # Should return 5 cities
    if len(result["result"]) == 5:
        return True, f"PASS: {query}"
    return False, f"FAIL: {query} - Expected 5 groups, got {len(result['result'])}"


def test_city_count_with_group_by(executor):
    """Test: SELECT city, COUNT(*) FROM users GROUP BY city"""
    query = "SELECT city, COUNT(*) FROM users GROUP BY city"
    result = run_query(executor, query)
    
    if "error" in result:
        return False, f"FAIL: {query} - Error: {result['error']}"
    
    # Should return 5 cities with counts
    if len(result["result"]) == 5:
        # Check that result has 'city' and 'count' keys
        first_row = result["result"][0]
        if 'city' in first_row and 'count' in first_row:
            return True, f"PASS: {query}"
    return False, f"FAIL: {query} - Invalid result structure"


def test_where_group_by(executor):
    """Test: SELECT city, COUNT(*) FROM users WHERE age > 25 GROUP BY city"""
    query = "SELECT city, COUNT(*) FROM users WHERE age > 25 GROUP BY city"
    result = run_query(executor, query)
    
    if "error" in result:
        return False, f"FAIL: {query} - Error: {result['error']}"
    
    # Should return groups filtered by WHERE
    if len(result["result"]) > 0:
        return True, f"PASS: {query}"
    return False, f"FAIL: {query} - No results returned"


def test_order_by_count(executor):
    """Test: SELECT city, COUNT(*) FROM users GROUP BY city ORDER BY COUNT(*)"""
    query = "SELECT city, COUNT(*) FROM users GROUP BY city ORDER BY COUNT(*)"
    result = run_query(executor, query)
    
    if "error" in result:
        return False, f"FAIL: {query} - Error: {result['error']}"
    
    # Should return 5 cities ordered by count (ascending)
    if len(result["result"]) == 5:
        # Verify it's ordered by count ascending
        counts = [row["count"] for row in result["result"]]
        if counts == sorted(counts):
            return True, f"PASS: {query}"
    return False, f"FAIL: {query} - ORDER BY COUNT(*) not working correctly"


def test_single_quoted_string(executor):
    """Test: SELECT name FROM users WHERE city = 'Madrid'"""
    query = "SELECT name FROM users WHERE city = 'Madrid'"
    result = run_query(executor, query)
    
    if "error" in result:
        return False, f"FAIL: {query} - Error: {result['error']}"
    
    # Should return 4 users from Madrid
    if len(result["result"]) == 4:
        return True, f"PASS: {query}"
    return False, f"FAIL: {query} - Expected 4 users, got {len(result['result'])}"


def test_fake_column_error(executor):
    """Test: SELECT city, SUM(fake_column) FROM users GROUP BY city should ERROR"""
    query = "SELECT city, SUM(fake_column) FROM users GROUP BY city"
    result = run_query(executor, query)
    
    if "error" in result:
        error_msg = result["error"]
        if "fake_column" in error_msg and "not found" in error_msg:
            return True, f"PASS: {query} - Correctly raises error"
        return False, f"FAIL: {query} - Wrong error message: {error_msg}"
    
    return False, f"FAIL: {query} - Should have raised error but got: {result['result']}"


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_tests():
    """Run all SQL engine fix verification tests."""
    # Setup
    database = create_test_database()
    storage = MemoryStorage(database)
    executor = QueryExecutor(storage)
    
    # Test cases
    tests = [
        ("COUNT(*) with GROUP BY", test_count_with_group_by),
        ("city + COUNT(*) with GROUP BY", test_city_count_with_group_by),
        ("WHERE + GROUP BY", test_where_group_by),
        ("ORDER BY COUNT(*)", test_order_by_count),
        ("Single quoted strings", test_single_quoted_string),
        ("Fake column error", test_fake_column_error),
    ]
    
    print("=" * 60)
    print("SQL ENGINE FIX VERIFICATION TESTS")
    print("=" * 60)
    print()
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        ok, msg = test_func(executor)
        print(msg)
        if ok:
            passed += 1
        else:
            failed += 1
    
    print()
    print("=" * 60)
    print(f"RESULTS: {passed} passed, {failed} failed, {passed + failed} total")
    print("=" * 60)
    
    return passed, failed


if __name__ == "__main__":
    run_all_tests()
