# SQL Executor - Query Execution Layer
# Executes query plans against a storage backend

from ast_nodes import Condition, LogicalCondition
from planner import SelectPlan


class QueryExecutor:
    """Minimal executor for SQL queries"""
    
    def __init__(self, storage):
        """
        Initialize executor with a storage object.
        
        Args:
            storage: Storage object with load_table method.
        """
        self.storage = storage
    
    def execute(self, plan):
        """
        Execute a query plan and return results.
        
        Args:
            plan: Query plan (SelectPlan, etc.)
            
        Returns:
            Query results (list of values for SELECT)
        """
        if isinstance(plan, SelectPlan):
            return self._execute_select(plan)
        else:
            raise ValueError(f"Unsupported plan: {type(plan).__name__}")
    
    def _execute_select(self, plan: SelectPlan):
        """
        Execute a SELECT query plan.
        
        Args:
            plan: SelectPlan object
            
        Returns:
            List of values from the requested columns, or full rows for SELECT *
        """
        table_name = plan.table
        columns = plan.columns
        where = plan.where
        order_by = plan.order_by
        
        # Load table from storage
        table = self.storage.load_table(table_name)
        rows = table.get_rows()
        
        # Filter rows based on WHERE condition
        if where is not None:
            def matches(row):
                return self._evaluate_condition(row, where)
            rows = [row for row in rows if matches(row)]
        
        # Sort rows based on ORDER BY column
        if order_by is not None:
            rows = sorted(rows, key=lambda r: r[order_by])
        
        # Handle SELECT *
        if columns == '*':
            return [row.copy() for row in rows]
        
        # Extract the requested columns from each row
        results = []
        for row in rows:
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
