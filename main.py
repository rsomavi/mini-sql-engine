from parser import get_parser
from planner import QueryPlanner
from executor import QueryExecutor
from storage import MemoryStorage
from ast_printer import print_ast
from ui import get_tokens, create_paginated_dashboard, create_simple_dashboard
import re
import sys
import io


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
    print("MiniSQL Engine with Rich Dashboard")
    print("Type 'exit' to quit\n")

    # REPL loop
    while True:
        try:
            query = input("sql> ")
        except EOFError:
            # Handle piped input ending
            print()
            break
        except KeyboardInterrupt:
            # Handle Ctrl+C gracefully
            print()
            break
        
        # Strip whitespace for processing
        query = query.strip()
        
        # Handle exit command (case-insensitive)
        if query.lower() == "exit":
            break
        
        # Ignore empty input
        if not query:
            continue
        
        # Strip trailing semicolon (only one, and no semicolons elsewhere)
        if query.endswith(';'):
            # Ensure no other semicolons exist (prevents multiple statements)
            if ';' in query[:-1]:
                print("ERROR: multiple statements not supported")
                tokens = get_tokens(query)
                create_paginated_dashboard(query, None, tokens, None, "ERROR: multiple statements not supported")
                continue
            query = query[:-1]
        
        try:
            # Capture parser output to suppress "Syntax error" messages
            old_stdout = sys.stdout
            sys.stdout = io.StringIO()
            
            # Generate AST
            ast = parser.parse(query)
            
            # Get captured output and restore stdout
            parser_output = sys.stdout.getvalue()
            sys.stdout = old_stdout
            
            # Check if parsing failed
            if ast is None:
                print("Syntax error")
                continue
            
            # Get tokens
            tokens = get_tokens(query)
            
            # Create query plan
            plan = planner.plan(ast)
            
            # Execute query
            result = executor.execute(plan)
            
            # Display dashboard with pagination
            create_paginated_dashboard(query, ast, tokens, result)
            
        except ValueError as e:
            error_msg = str(e)
            if "Table not found" in error_msg:
                table_name = error_msg.split(":")[-1].strip()
                error_msg = f"ERROR: table '{table_name}' does not exist"
            elif "Column" in error_msg and "not found" in error_msg:
                match = re.search(r"Column '(\w+)'", error_msg)
                if match:
                    col_name = match.group(1)
                    error_msg = f"ERROR: column '{col_name}' does not exist"
                else:
                    error_msg = "ERROR: query execution failed"
            else:
                error_msg = f"ERROR: {error_msg}"
            
            # Get tokens even for error case
            tokens = get_tokens(query)
            create_paginated_dashboard(query, None, tokens, None, error_msg)
            
        except Exception as e:
            # Handle any other exceptions gracefully
            print(f"Error: {type(e).__name__}: {e}")
            tokens = get_tokens(query)
            create_paginated_dashboard(query, None, tokens, None, "ERROR: invalid SQL syntax")


if __name__ == "__main__":
    main()
