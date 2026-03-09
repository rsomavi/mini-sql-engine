from parser import get_parser
from executor import QueryExecutor

def main():

    # Query que quieres ejecutar
    query = "SELECT name FROM users"

    # Base de datos en memoria
    database = {
        "users": [
            {"name": "Juan"},
            {"name": "Ana"},
            {"name": "Luis"}
        ]
    }

    # Crear parser
    parser = get_parser()

    # Generar AST
    ast = parser.parse(query)

    print("AST:", ast)

    # Crear executor
    executor = QueryExecutor(database)

    # Ejecutar query
    result = executor.execute(ast)

    print("Result:", result)

if __name__ == "__main__":
    main()