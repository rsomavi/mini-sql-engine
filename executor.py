# SQL Executor - Query Execution Layer
# Executes AST nodes against an in-memory database

from ast_nodes import SelectQuery, Condition, LogicalCondition

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
            List of values from the requested columns, or full rows for SELECT *
        """
        table_name = select_node.table
        columns = select_node.columns
        where = select_node.where
        
        # Check if table exists
        if table_name not in self.database:
            raise ValueError(f"Table not found: {table_name}")
        
        table = self.database[table_name]
        
        # Filter rows based on WHERE condition
        if where is not None:
            def matches(row):
                return self._evaluate_condition(row, where)
            table = [row for row in table if matches(row)]
        
        # Handle SELECT *
        if columns == '*':
            return table
        
        # Extract the requested columns from each row
        results = []
        for row in table:
            result_row = {}
            for column_name in columns:
                if column_name not in row:
                    raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")
                result_row[column_name] = row[column_name]
            results.append(result_row)
        
        return results
    
    def _evaluate_condition(self, row, condition):
        """Recursively evaluate a condition (simple or logical)."""
        if isinstance(condition, Condition):
            col_value = row.get(condition.column)
            if col_value is None:
                return False
            op = condition.operator
            value = condition.value
            if op == '=':
                return col_value == value
            elif op == '>':
                return col_value > value
            elif op == '<':
                return col_value < value
            elif op == '>=':
                return col_value >= value
            elif op == '<=':
                return col_value <= value
            else:
                raise ValueError(f"Unknown operator: {op}")
        elif isinstance(condition, LogicalCondition):
            left_result = self._evaluate_condition(row, condition.left)
            right_result = self._evaluate_condition(row, condition.right)
            if condition.operator == 'AND':
                return left_result and right_result
            elif condition.operator == 'OR':
                return left_result or right_result
            else:
                raise ValueError(f"Unknown logical operator: {condition.operator}")
        else:
            raise ValueError(f"Unknown condition type: {type(condition).__name__}")
