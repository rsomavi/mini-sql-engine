from parser import get_parser
from planner import QueryPlanner
from executor import QueryExecutor
from storage_disk import DiskStorage
from storage_server import ServerStorage
from ast_printer import print_ast
from ui import get_tokens, create_paginated_dashboard, create_simple_dashboard
import re
import sys
import io


def main():
    # Create parser, planner, storage, and executor
    parser = get_parser()
    planner = QueryPlanner()
    storage = ServerStorage()
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
