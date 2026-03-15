#!/usr/bin/env python3
"""
Test runner for all SQL engine tests.
Executes all test files and displays a summary.
"""

import subprocess
import sys
import os


def run_test_file(test_file):
    """Run a single test file and return the result."""
    print(f"\n{'=' * 60}")
    print(f"Running: {test_file}")
    print('=' * 60)
    
    result = subprocess.run(
        [sys.executable, test_file],
        capture_output=True,
        text=True
    )
    
    # Print the output
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    return result.returncode == 0


def main():
    """Run all test files and display summary."""
    print("=" * 60)
    print("SQL ENGINE TEST SUITE")
    print("=" * 60)
    
    # Get the tests directory
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Test files to run
    test_files = [
        "test_group_by.py",
        "test_queries.py",
        "test_sql_engine.py"
    ]
    
    results = {}
    
    for test_file in test_files:
        test_path = os.path.join(tests_dir, test_file)
        
        if not os.path.exists(test_path):
            print(f"\n⚠️  SKIP: {test_file} not found")
            results[test_file] = False
            continue
        
        success = run_test_file(test_path)
        results[test_file] = success
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for test_file, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"{status}: {test_file}")
        if not passed:
            all_passed = False
    
    print()
    print("=" * 60)
    if all_passed:
        print("ALL TESTS PASSED")
    else:
        print("SOME TESTS FAILED")
    print("=" * 60)
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
