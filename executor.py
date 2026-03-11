# SQL Executor - Query Execution Layer
# Executes query plans against a storage backend

from ast_nodes import Condition, LogicalCondition, NotCondition
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
        limit = plan.limit
        
        # Load table from storage
        table = self.storage.load_table(table_name)
        rows = table.get_rows()
        
        # Filter rows based on WHERE condition
        if where is not None:
            def matches(row):
                return self._evaluate_condition(row, where, table_name)
            rows = [row for row in rows if matches(row)]
        
        # Validate ORDER BY column exists
        if order_by is not None:
            # Check if order_by column exists in any row (use first row as reference)
            if rows:
                if order_by not in rows[0]:
                    raise ValueError(f"Column '{order_by}' not found in table '{table_name}'")
            else:
                # If no rows, check against table schema if available
                # For now, we'll skip validation if table is empty
                pass
        
        # Sort rows based on ORDER BY column
        if order_by is not None:
            rows = sorted(rows, key=lambda r: r[order_by])
        
        # Apply LIMIT
        if limit is not None:
            rows = rows[:limit]
        
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
    
    def _evaluate_condition(self, row, condition, table_name):
        """Recursively evaluate a condition (simple or logical)."""
        if isinstance(condition, Condition):
            col_value = row.get(condition.column)
            if col_value is None:
                raise ValueError(f"Column '{condition.column}' not found in table '{table_name}'")
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
            left_result = self._evaluate_condition(row, condition.left, table_name)
            right_result = self._evaluate_condition(row, condition.right, table_name)
            op = condition.operator.upper()
            if op == 'AND':
                return left_result and right_result
            elif op == 'OR':
                return left_result or right_result
            else:
                raise ValueError(f"Unknown logical operator: {condition.operator}")
        elif isinstance(condition, NotCondition):
            return not self._evaluate_condition(row, condition.condition, table_name)
        else:
            raise ValueError(f"Unknown condition type: {type(condition).__name__}")
