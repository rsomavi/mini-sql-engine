from parser import get_parser
from executor import QueryExecutor


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
        "SELECT name FROM users",
        "SELECT name, age FROM users",
        "SELECT name, city FROM users",
        "SELECT * FROM users",
        "SELECT name, price FROM products",
        "SELECT * FROM products",
        "SELECT name FROM users WHERE age = 25",
        "SELECT name, city FROM users WHERE age = 30",
        "SELECT name, city FROM users WHERE age = 30",
        "SELECT * FROM users WHERE age = 28",
        'SELECT name, city FROM users WHERE name = "Ana"'
    ]

    # Create parser and executor
    parser = get_parser()
    executor = QueryExecutor(database)

    # Execute each query
    for query in queries:
        print(f"Query: {query}")
        
        # Generate AST
        ast = parser.parse(query)
        print(f"AST: {ast}")
        
        # Execute query
        result = executor.execute(ast)
        print(f"Result: {result}")
        print()


if __name__ == "__main__":
    main()
