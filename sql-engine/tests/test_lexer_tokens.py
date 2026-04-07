#!/usr/bin/env python3
# test_lexer_tokens.py — verifica que el lexer tokeniza correctamente
# Run desde sql-engine/: python3 tests/test_lexer_tokens.py

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from lexer import get_lexer

tests_passed = 0
tests_failed = 0

def check_tokens(query, expected_types):
    global tests_passed, tests_failed
    lexer = get_lexer()
    lexer.lexer.input(query)
    actual = []
    while True:
        tok = lexer.lexer.token()
        if not tok: break
        actual.append(tok.type)
    
    if actual == expected_types:
        print(f"  PASS: {query[:60]}")
        tests_passed += 1
    else:
        print(f"  FAIL: {query[:60]}")
        print(f"        expected: {expected_types}")
        print(f"        got:      {actual}")
        tests_failed += 1

print("=== TEST LEXER TOKENS ===\n")

print("-- SELECT básico --")
check_tokens(
    "SELECT * FROM users",
    ['SELECT', 'STAR', 'FROM', 'ID']
)
check_tokens(
    "SELECT id, name FROM users",
    ['SELECT', 'ID', 'COMMA', 'ID', 'FROM', 'ID']
)
check_tokens(
    "SELECT DISTINCT city FROM users",
    ['SELECT', 'DISTINCT', 'ID', 'FROM', 'ID']
)

print("\n-- WHERE --")
check_tokens(
    "SELECT * FROM users WHERE age > 25",
    ['SELECT', 'STAR', 'FROM', 'ID', 'WHERE', 'ID', 'GT', 'NUMBER']
)
check_tokens(
    "SELECT * FROM users WHERE city = 'Madrid'",
    ['SELECT', 'STAR', 'FROM', 'ID', 'WHERE', 'ID', 'EQUAL', 'STRING']
)
check_tokens(
    "SELECT * FROM users WHERE age >= 18 AND age <= 30",
    ['SELECT', 'STAR', 'FROM', 'ID', 'WHERE', 'ID', 'GE', 'NUMBER', 'AND', 'ID', 'LE', 'NUMBER']
)
check_tokens(
    "SELECT * FROM users WHERE NOT city = 'Madrid'",
    ['SELECT', 'STAR', 'FROM', 'ID', 'WHERE', 'NOT', 'ID', 'EQUAL', 'STRING']
)

print("\n-- ORDER BY, LIMIT --")
check_tokens(
    "SELECT * FROM users ORDER BY age ASC",
    ['SELECT', 'STAR', 'FROM', 'ID', 'ORDER', 'BY', 'ID', 'ASC']
)
check_tokens(
    "SELECT * FROM users ORDER BY age DESC LIMIT 5",
    ['SELECT', 'STAR', 'FROM', 'ID', 'ORDER', 'BY', 'ID', 'DESC', 'LIMIT', 'NUMBER']
)

print("\n-- GROUP BY, HAVING --")
check_tokens(
    "SELECT city FROM users GROUP BY city",
    ['SELECT', 'ID', 'FROM', 'ID', 'GROUP', 'BY', 'ID']
)
check_tokens(
    "SELECT city FROM users GROUP BY city HAVING COUNT(*) > 2",
    ['SELECT', 'ID', 'FROM', 'ID', 'GROUP', 'BY', 'ID', 'HAVING', 'COUNT', 'LPAREN', 'STAR', 'RPAREN', 'GT', 'NUMBER']
)

print("\n-- Agregaciones --")
check_tokens(
    "SELECT COUNT(*) FROM users",
    ['SELECT', 'COUNT', 'LPAREN', 'STAR', 'RPAREN', 'FROM', 'ID']
)
check_tokens(
    "SELECT SUM(age) FROM users",
    ['SELECT', 'SUM', 'LPAREN', 'ID', 'RPAREN', 'FROM', 'ID']
)
check_tokens(
    "SELECT AVG(age), MIN(age), MAX(age) FROM users",
    ['SELECT', 'AVG', 'LPAREN', 'ID', 'RPAREN', 'COMMA', 'MIN', 'LPAREN', 'ID', 'RPAREN', 'COMMA', 'MAX', 'LPAREN', 'ID', 'RPAREN', 'FROM', 'ID']
)

print("\n-- JOIN --")
check_tokens(
    "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    ['SELECT', 'STAR', 'FROM', 'ID', 'JOIN', 'ID', 'ON', 'ID', 'DOT', 'ID', 'EQUAL', 'ID', 'DOT', 'ID']
)

print("\n-- CREATE TABLE --")
check_tokens(
    "CREATE TABLE test (id INT PRIMARY KEY)",
    ['CREATE', 'TABLE', 'ID', 'LPAREN', 'ID', 'INT', 'PRIMARY', 'KEY', 'RPAREN']
)
check_tokens(
    "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, age INT)",
    ['CREATE', 'TABLE', 'ID', 'LPAREN',
     'ID', 'INT', 'PRIMARY', 'KEY', 'COMMA',
     'ID', 'VARCHAR', 'LPAREN', 'NUMBER', 'RPAREN', 'NOT', 'NULL', 'COMMA',
     'ID', 'INT',
     'RPAREN']
)
check_tokens(
    "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price FLOAT, active BOOL)",
    ['CREATE', 'TABLE', 'ID', 'LPAREN',
     'ID', 'INT', 'PRIMARY', 'KEY', 'COMMA',
     'ID', 'VARCHAR', 'LPAREN', 'NUMBER', 'RPAREN', 'COMMA',
     'ID', 'FLOAT', 'COMMA',
     'ID', 'BOOL',
     'RPAREN']
)

print(f"\n=== RESULT: {tests_passed} passed, {tests_failed} failed, {tests_passed+tests_failed} total ===")