# SQL Executor - Query Execution Layer
# Executes AST nodes against an in-memory database

from ast_nodes import SelectQuery

class QueryExecutor:
    """Minimal executor for SQL queries"""
    
    def __init__(self, database: dict):
        """
        Initialize executor with in-memory database.
        
        Args:
            database: Dict mapping table names to list of rows.
                     Example: {"users": [{"name": "Juan"}, {"name": "Ana"}]}
        """
        self.database = database
    
    def execute(self, ast):
        """
        Execute an AST node and return results.
        
        Args:
            ast: AST node (SelectQuery, etc.)
            
        Returns:
            Query results (list of values for SELECT)
        """
        if isinstance(ast, SelectQuery):
            return self._execute_select(ast)
        else:
            raise ValueError(f"Unsupported AST node: {type(ast).__name__}")
    
    def _execute_select(self, select_node: SelectQuery):
        """
        Execute a SELECT query.
        
        Args:
            select_node: SelectQuery AST node
            
        Returns:
            List of values from the requested column
        """
        table_name = select_node.table
        column_name = select_node.column
        
        # Check if table exists
        if table_name not in self.database:
            raise ValueError(f"Table not found: {table_name}")
        
        table = self.database[table_name]
        
        # Extract the requested column from each row
        results = []
        for row in table:
            if column_name not in row:
                raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")
            results.append(row[column_name])
        
        return results
