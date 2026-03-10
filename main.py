from parser import get_parser
from planner import QueryPlanner
from executor import QueryExecutor
from storage import MemoryStorage


def main():
    # In-memory database with multiple tables
    database = {
        "users": [
            {"id": 1, "name": "Juan", "age": 25, "city": "Madrid"},
            {"id": 2, "name": "Ana", "age": 30, "city": "Barcelona"},
            {"id": 3, "name": "Luis", "age": 22, "city": "Valencia"},
            {"id": 4, "name": "Maria", "age": 28, "city": "Sevilla"}
        ],
        "products": [
            {"id": 1, "name": "Laptop", "price": 1200},
            {"id": 2, "name": "Phone", "price": 800},
            {"id": 3, "name": "Keyboard", "price": 100}
        ]
    }

    # List of queries to test
    queries = [
        # Error cases
        "SELECT id FROM hola",
        "SELECT mesa FROM users",
        "SELECT id name FROM users",
        # Valid queries
        "SELECT name FROM users",
        "SELECT name, age FROM users",
        "SELECT name, city FROM users",
        "SELECT * FROM users",
        "SELECT name, price FROM products",
        "SELECT * FROM products",
        "SELECT name FROM users WHERE age = 25",
        "SELECT name, city FROM users WHERE age = 30",
        "SELECT * FROM users WHERE age = 28",
        'SELECT name, city FROM users WHERE name = "Ana"',
        "SELECT name, age FROM users WHERE age > 25",
        "SELECT id, name, city FROM users WHERE age >= 28",
        "SELECT name, city FROM users WHERE age < 30",
        'SELECT * FROM users WHERE name = "Luis"',
        "SELECT name FROM products WHERE price > 500",
        "SELECT name, price FROM products WHERE price <= 800",
        "SELECT * FROM products WHERE price >= 100",
        "SELECT name FROM users WHERE age > 100",
        "SELECT name FROM users WHERE age >= 30",
        'SELECT name FROM users WHERE age > 20 AND city = "Madrid"',
        'SELECT name FROM users WHERE age >= 28 AND city = "Sevilla"',
        'SELECT name FROM users WHERE city = "Madrid" OR city = "Barcelona"',
        'SELECT name FROM users WHERE age < 23 OR city = "Sevilla"',
        'SELECT name, age FROM users WHERE age > 20 AND age < 30',
        'SELECT * FROM users WHERE age > 25 AND city = "Sevilla"',
        'SELECT * FROM users WHERE city = "Madrid" OR city = "Valencia"',
        'SELECT name FROM products WHERE price >= 100 AND price <= 800',
        'SELECT name FROM products WHERE price = 100 OR price = 1200',
        # ORDER BY queries
        "SELECT name FROM users ORDER BY age",
        "SELECT name, age FROM users ORDER BY age",
        "SELECT * FROM users ORDER BY age",
        "SELECT name FROM users WHERE age > 20 ORDER BY age",
        "SELECT name FROM products ORDER BY price",
        "SELECT id, name FROM users ORDER BY id",
    ]

    # Create parser, planner, storage, and executor
    parser = get_parser()
    planner = QueryPlanner()
    storage = MemoryStorage(database)
    executor = QueryExecutor(storage)

    # Execute each query
    for query in queries:
        print(f"Query: {query}")
        
        try:
            # Generate AST
            ast = parser.parse(query)
            if ast is None:
                print("ERROR: invalid SQL syntax")
                print()
                continue
            print(f"AST: {ast}")
            
            # Create query plan
            plan = planner.plan(ast)
            print(f"Plan: {plan}")
            
            # Execute query
            result = executor.execute(plan)
            print(f"Result: {result}")
        except ValueError as e:
            error_msg = str(e)
            if "Table not found" in error_msg:
                table_name = error_msg.split(":")[-1].strip()
                print(f"ERROR: table '{table_name}' does not exist")
            elif "Column" in error_msg and "not found" in error_msg:
                # Extract column name from error message
                import re
                match = re.search(r"Column '(\w+)'", error_msg)
                if match:
                    col_name = match.group(1)
                    print(f"ERROR: column '{col_name}' does not exist")
                else:
                    print("ERROR: query execution failed")
            else:
                print("ERROR: query execution failed")
        except Exception:
            print("ERROR: invalid SQL syntax")
        
        print()


if __name__ == "__main__":
    main()
