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

    # Create parser, planner, storage, and executor
    parser = get_parser()
    planner = QueryPlanner()
    storage = MemoryStorage(database)
    executor = QueryExecutor(storage)

    # Print welcome message
    print("\033[H\033[2J", end="")
    print("MiniSQL Engine")
    print("Type 'exit' to quit\n")

    # REPL loop
    while True:
        query = input("sql> ").strip()
        
        if query.lower() == "exit":
            break
        
        if not query:
            continue
        
        try:
            # Generate AST
            ast = parser.parse(query)
            if ast is None:
                print("ERROR: invalid SQL syntax")
                continue
            
            # Create query plan
            plan = planner.plan(ast)
            
            # Execute query
            result = executor.execute(plan)
            print(f"Result: {result}")
        except ValueError as e:
            error_msg = str(e)
            if "Table not found" in error_msg:
                table_name = error_msg.split(":")[-1].strip()
                print(f"ERROR: table '{table_name}' does not exist")
            elif "Column" in error_msg and "not found" in error_msg:
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


if __name__ == "__main__":
    main()
