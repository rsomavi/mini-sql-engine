# SQL Executor - Query Execution Layer
# Executes query plans against a storage backend

from ast_nodes import Condition, LogicalCondition, NotCondition
from planner import SelectPlan, CountPlan, SumPlan, AvgPlan, MinPlan, MaxPlan, \
                    CreateTablePlan, InsertPlan, DeletePlan, UpdatePlan


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
            Query results (list of values for SELECT) or count (int)
        """
        if isinstance(plan, SelectPlan):
            return self._execute_select(plan)
        elif isinstance(plan, CountPlan):
            return self._execute_count(plan)
        elif isinstance(plan, SumPlan):
            return self._execute_sum(plan)
        elif isinstance(plan, AvgPlan):
            return self._execute_avg(plan)
        elif isinstance(plan, MinPlan):
            return self._execute_min(plan)
        elif isinstance(plan, MaxPlan):
            return self._execute_max(plan)
        elif isinstance(plan, CreateTablePlan):
            return self._execute_create(plan)
        elif isinstance(plan, InsertPlan):
            return self._execute_insert(plan)
        elif isinstance(plan, DeletePlan):
            return self._execute_delete(plan)
        elif isinstance(plan, UpdatePlan):
            return self._execute_update(plan)
        else:
            raise ValueError(f"Unsupported plan: {type(plan).__name__}")
    
    # =========================================================================
    # Helper: Common table loading and WHERE filtering
    # =========================================================================
    
    def _get_filtered_rows(self, table_name, where):
        """
        Load table and optionally filter by WHERE condition.
        
        Args:
            table_name: Name of the table
            where: Optional WHERE condition (Condition, LogicalCondition, NotCondition)
            
        Returns:
            List of filtered rows
        """
        table = self.storage.load_table(table_name)
        rows = table.get_rows()
        
        if where is not None:
            def matches(row):
                return self._evaluate_condition(row, where, table_name)
            rows = [row for row in rows if matches(row)]
        
        return rows
    
    # =========================================================================
    # Condition evaluation
    # =========================================================================
    
    def _evaluate_condition(self, row, condition, table_name):
        """Recursively evaluate a condition (simple or logical)."""
        if isinstance(condition, Condition):
            if condition.column not in row:
                raise ValueError(f"Column '{condition.column}' not found in table '{table_name}'")
            col_value = row[condition.column]
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
    
    # =========================================================================
    # SELECT execution and helpers
    # =========================================================================
    
    def _execute_select(self, plan: SelectPlan):
        """
        Execute a SELECT query plan.
        
        Args:
            plan: SelectPlan object
            
        Returns:
            List of values from the requested columns, or full rows for SELECT *
        """
        # Apply JOIN first if specified
        if plan.join_table:
            rows = self._apply_join(plan)
        else:
            # Load and filter rows from single table
            rows = self._get_filtered_rows(plan.table, plan.where)
        
        # Apply GROUP BY if specified
        if plan.group_by:
            rows = self._apply_group_by(rows, plan)
        else:
            # Apply transformations in order for non-GROUP BY queries
            rows = self._apply_distinct(rows, plan)
            rows = self._apply_order_by(rows, plan)
            rows = self._apply_limit(rows, plan)
        
        return self._apply_projection(rows, plan)
    
    def _apply_join(self, plan: SelectPlan):
        """
        Apply INNER JOIN to rows from two tables.
        
        Args:
            plan: SelectPlan object with join_table and join_condition
            
        Returns:
            List of merged rows from both tables
        """
        join_table = plan.join_table
        join_condition = plan.join_condition  # Tuple: (left_info, right_info) where each is {'table': 'x', 'column': 'y'}
        
        if not join_table or not join_condition:
            return self._get_filtered_rows(plan.table, plan.where)
        
        left_info, right_info = join_condition
        left_col = left_info['column']
        right_col = right_info['column']
        
        # Load rows from both tables
        left_rows = self._get_filtered_rows(plan.table, None)  # No WHERE yet - applied after join
        right_rows = self._get_filtered_rows(join_table, None)
        
        # Nested Loop Join
        results = []
        for row1 in left_rows:
            for row2 in right_rows:
                # Check join condition: row1[left_col] == row2[right_col]
                left_val = row1.get(left_col)
                right_val = row2.get(right_col)
                if left_val is not None and right_val is not None and left_val == right_val:
                    # Merge rows
                    merged_row = {**row1, **row2}
                    results.append(merged_row)
        
        # Apply WHERE after join if present
        if plan.where:
            results = [row for row in results if self._evaluate_condition(row, plan.where, plan.table)]
        
        return results
    
    def _apply_group_by(self, rows, plan: SelectPlan):
        """
        Apply GROUP BY to rows and compute aggregates.
        
        Args:
            rows: List of filtered rows
            plan: SelectPlan object
            
        Returns:
            List of aggregated rows
        """
        group_by = plan.group_by
        
        # Normalize group_by to always be a list (supports both single column and multiple)
        if isinstance(group_by, str):
            group_by_columns = [group_by]
        else:
            group_by_columns = list(group_by)
        
        # Validate GROUP BY columns exist
        if rows:
            for col in group_by_columns:
                if col not in rows[0]:
                    raise ValueError(f"Column '{col}' not found in table '{plan.table}'")
        
        # =========================================================================
        # SQL STANDARD VALIDATION: GROUP BY columns must be in SELECT or aggregated
        # =========================================================================
        # In SQL, every column in SELECT must either:
        # 1. Be in the GROUP BY clause, OR
        # 2. Be inside an aggregate function (COUNT, SUM, AVG, MIN, MAX)
        # =========================================================================
        
        # Helper function to normalize column names: 'users.city' -> 'city'
        def normalize_column(col):
            if isinstance(col, str) and '.' in col:
                return col.split('.')[-1]
            return col
        
        columns = plan.columns
        group_by_columns_set = set(normalize_column(c) for c in group_by_columns)
        
        if isinstance(columns, list):
            for col in columns:
                # Get column name (handle both string and dict formats)
                if isinstance(col, dict):
                    if col.get('type') == 'aggregate':
                        # Validate aggregate column exists
                        agg_col = col.get('column')
                        if agg_col and rows and agg_col not in rows[0]:
                            raise ValueError(f"Column '{agg_col}' not found in table '{plan.table}'")
                        continue
                    col_name = col.get('name', col.get('column'))
                else:
                    col_name = col
                
                # Normalize and check if column is in GROUP BY
                normalized_col = normalize_column(col_name)
                if normalized_col not in group_by_columns_set:
                    raise ValueError(
                        f"Column '{col_name}' must be in GROUP BY or be aggregated. "
                        f"GROUP BY columns: {group_by_columns}"
                    )
        
        # Group rows by the group_by columns (using tuple key for multiple columns)
        groups = {}
        for row in rows:
            # Create tuple key from multiple columns: key = (row["city"], row["age"])
            key = tuple(row[col] for col in group_by_columns)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # Compute aggregates for each group
        results = []
        columns = plan.columns
        
        # Extract aggregate info from plan
        sum_col = getattr(plan, 'sum_column', None)
        avg_col = getattr(plan, 'avg_column', None)
        min_col = getattr(plan, 'min_column', None)
        max_col = getattr(plan, 'max_column', None)
        
        for key, rows_in_group in groups.items():
            # Build result row with all group by columns
            if len(group_by_columns) == 1:
                result_row = {group_by_columns[0]: key[0] if isinstance(key, tuple) else key}
            else:
                # Multiple columns: key is a tuple
                result_row = {}
                for i, col in enumerate(group_by_columns):
                    result_row[col] = key[i]
            
            # Handle each column - check if it's an aggregate function
            if isinstance(columns, list):
                for col in columns:
                    if isinstance(col, dict) and col.get('type') == 'aggregate':
                        func = col.get('func')
                        col_name = col.get('column')
                        
                        if func == 'count':
                            result_row['count'] = len(rows_in_group)
                        elif func == 'sum' and col_name:
                            total = sum(row.get(col_name, 0) for row in rows_in_group)
                            result_row['sum'] = total
                        elif func == 'avg' and col_name:
                            values = [row.get(col_name, 0) for row in rows_in_group]
                            result_row['avg'] = sum(values) / len(values) if values else 0
                        elif func == 'min' and col_name:
                            values = [row.get(col_name) for row in rows_in_group if col_name in row]
                            result_row['min'] = min(values) if values else None
                        elif func == 'max' and col_name:
                            values = [row.get(col_name) for row in rows_in_group if col_name in row]
                            result_row['max'] = max(values) if values else None
                    elif isinstance(col, str):
                        # Regular column - for GROUP BY, take the first value
                        result_row[col] = rows_in_group[0].get(col)
            
            results.append(result_row)
        
        # Apply HAVING after grouping (filter groups based on aggregate conditions)
        if plan.having:
            results = self._apply_having(results, plan)
        
        # Apply ORDER BY and LIMIT after grouping
        results = self._apply_order_by(results, plan)
        results = self._apply_limit(results, plan)
        
        return results
    
    def _apply_distinct(self, rows, plan: SelectPlan):
        """Apply DISTINCT to rows if specified."""
        distinct = getattr(plan, 'distinct', False)
        if not distinct:
            return rows
        
        columns = plan.columns
        seen = set()
        unique_rows = []
        for row in rows:
            # Create key from all column values (convert to strings for hashability)
            if columns == '*':
                key = tuple((k, str(v)) for k, v in sorted(row.items()))
            else:
                # Handle both string columns and dict columns
                key_values = []
                for col in columns:
                    if isinstance(col, dict):
                        col_name = col.get('name', col.get('column'))
                    else:
                        col_name = col
                    key_values.append(str(row.get(col_name)))
                key = tuple(key_values)
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)
        return unique_rows
    
    def _apply_order_by(self, rows, plan: SelectPlan):
        """Apply ORDER BY to rows if specified."""
        order_by = plan.order_by
        if order_by is None:
            return rows
        
        # Handle ORDER BY with aggregate (e.g., ORDER BY COUNT(*))
        if isinstance(order_by, dict) and order_by.get('type') == 'aggregate':
            func = order_by.get('func')
            col_name = order_by.get('column')  # May be None for COUNT(*)
            direction = order_by.get('direction', 'asc')  # Default to ASC
            
            # Compute aggregate for each row
            def get_aggregate_value(row):
                if func == 'count':
                    return row.get('count', 0)
                elif func == 'sum':
                    return row.get('sum', 0)
                elif func == 'avg':
                    return row.get('avg', 0)
                elif func == 'min':
                    return row.get('min', 0)
                elif func == 'max':
                    return row.get('max', 0)
                else:
                    raise ValueError(f"Unknown aggregate function: {func}")
            
            reverse = (direction == 'desc')
            return sorted(rows, key=get_aggregate_value, reverse=reverse)
        
        # Handle ORDER BY column - now returns tuple (column, direction)
        if isinstance(order_by, tuple):
            column, direction = order_by
            reverse = (direction == 'desc')
        else:
            # Backward compatibility: plain column name (legacy)
            column = order_by
            reverse = False
        
        # Validate ORDER BY column exists
        if rows:
            if column not in rows[0]:
                raise ValueError(f"Column '{column}' not found in table '{plan.table}'")
        
        return sorted(rows, key=lambda r: r[column], reverse=reverse)
    
    def _apply_limit(self, rows, plan: SelectPlan):
        """Apply LIMIT to rows if specified."""
        limit = plan.limit
        if limit is None:
            return rows
        return rows[:limit]
    
    def _apply_having(self, rows, plan: SelectPlan):
        """
        Apply HAVING condition to filter groups.
        
        Args:
            rows: List of aggregated rows (after GROUP BY)
            plan: SelectPlan object with having condition
            
        Returns:
            Filtered list of rows that satisfy HAVING condition
        """
        having = plan.having
        if having is None:
            return rows
        
        # Parse having condition: {'type': 'aggregate', 'func': 'count', 'operator': '>', 'value': 2}
        func = having.get('func')
        operator = having.get('operator')
        threshold = having.get('value')
        
        filtered_rows = []
        for row in rows:
            # Get the aggregate value from the row
            if func == 'count':
                agg_value = row.get('count', 0)
            elif func == 'sum':
                agg_value = row.get('sum', 0)
            elif func == 'avg':
                agg_value = row.get('avg', 0)
            elif func == 'min':
                agg_value = row.get('min', 0)
            elif func == 'max':
                agg_value = row.get('max', 0)
            else:
                raise ValueError(f"Unknown aggregate function in HAVING: {func}")
            
            # Evaluate the condition
            if operator == '>':
                if agg_value > threshold:
                    filtered_rows.append(row)
            elif operator == '<':
                if agg_value < threshold:
                    filtered_rows.append(row)
            elif operator == '>=':
                if agg_value >= threshold:
                    filtered_rows.append(row)
            elif operator == '<=':
                if agg_value <= threshold:
                    filtered_rows.append(row)
            elif operator == '=':
                if agg_value == threshold:
                    filtered_rows.append(row)
            else:
                raise ValueError(f"Unknown operator in HAVING: {operator}")
        
        return filtered_rows
    
    def _apply_projection(self, rows, plan: SelectPlan):
        """Apply column projection (SELECT columns)."""
        # If GROUP BY was used, rows already have the correct structure
        if plan.group_by:
            return rows
        
        columns = plan.columns
        table_name = plan.table
        
        # Handle SELECT *
        if columns == '*':
            return [row.copy() for row in rows]
        
        # Extract the requested columns from each row
        # Handle both string columns and dict columns (from parser)
        results = []
        for row in rows:
            result_row = {}
            for col in columns:
                # Handle dict format from parser: {'type': 'column', 'name': 'table.column', 'table': 'table', 'column': 'col'}
                if isinstance(col, dict):
                    if col.get('type') == 'column':
                        # Check if it's a qualified name (table.column)
                        if col.get('table'):
                            column_name = col.get('column')  # Use the actual column name
                            output_name = col.get('name')  # Use qualified name as output
                        else:
                            column_name = col.get('name')
                            output_name = col.get('name')
                    else:
                        column_name = col.get('name', col.get('column'))
                        output_name = column_name
                else:
                    column_name = col
                    output_name = col
                
                if column_name not in row:
                    raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")
                result_row[output_name] = row[column_name]
            results.append(result_row)
        
        return results
    
    # =========================================================================
    # Aggregation execution
    # =========================================================================
    
    def _execute_count(self, plan: CountPlan):
        """
        Execute a COUNT(*) query plan.
        
        Args:
            plan: CountPlan object
            
        Returns:
            Integer count of rows, or list of counts if GROUP BY
        """
        rows = self._get_filtered_rows(plan.table, plan.where)
        
        # Handle GROUP BY
        if plan.group_by:
            return self._execute_count_with_group_by(rows, plan)
        
        return [{"count": len(rows)}]
    
    def _execute_count_with_group_by(self, rows, plan: CountPlan):
        """Execute COUNT(*) with GROUP BY (supports multiple columns)."""
        group_by = plan.group_by
        
        # Normalize to list
        if isinstance(group_by, str):
            group_by_columns = [group_by]
        else:
            group_by_columns = list(group_by)
        
        # Validate GROUP BY columns exist
        if rows:
            for col in group_by_columns:
                if col not in rows[0]:
                    raise ValueError(f"Column '{col}' not found in table '{plan.table}'")
        
        # Group rows using tuple key
        groups = {}
        for row in rows:
            key = tuple(row[col] for col in group_by_columns)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # Return counts per group
        results = []
        for key, rows_in_group in groups.items():
            if len(group_by_columns) == 1:
                results.append({group_by_columns[0]: key[0] if isinstance(key, tuple) else key, 'count': len(rows_in_group)})
            else:
                result_row = {}
                for i, col in enumerate(group_by_columns):
                    result_row[col] = key[i]
                result_row['count'] = len(rows_in_group)
                results.append(result_row)
        
        return results
    
    def _execute_sum(self, plan: SumPlan):
        """
        Execute a SUM(column) query plan.
        
        Args:
            plan: SumPlan object
            
        Returns:
            Sum of column values, or list of sums if GROUP BY
        """
        rows = self._get_filtered_rows(plan.table, plan.where)
        
        # Validate column exists
        if rows and plan.column not in rows[0]:
            raise ValueError(f"Column '{plan.column}' not found in table '{plan.table}'")
        
        # Handle GROUP BY
        if plan.group_by:
            return self._execute_sum_with_group_by(rows, plan)
        
        total = 0
        for row in rows:
            total += row[plan.column]
        
        return [{"sum": total}]
    
    def _execute_sum_with_group_by(self, rows, plan: SumPlan):
        """Execute SUM(column) with GROUP BY (supports multiple columns)."""
        group_by = plan.group_by
        
        # Normalize to list
        if isinstance(group_by, str):
            group_by_columns = [group_by]
        else:
            group_by_columns = list(group_by)
        
        # Validate GROUP BY columns exist
        if rows:
            for col in group_by_columns:
                if col not in rows[0]:
                    raise ValueError(f"Column '{col}' not found in table '{plan.table}'")
        
        # Group rows using tuple key
        groups = {}
        for row in rows:
            key = tuple(row[col] for col in group_by_columns)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # Return sums per group
        results = []
        for key, rows_in_group in groups.items():
            if len(group_by_columns) == 1:
                results.append({group_by_columns[0]: key[0] if isinstance(key, tuple) else key, 'sum': sum(row[plan.column] for row in rows_in_group)})
            else:
                result_row = {}
                for i, col in enumerate(group_by_columns):
                    result_row[col] = key[i]
                result_row['sum'] = sum(row[plan.column] for row in rows_in_group)
                results.append(result_row)
        
        return results
    
    def _execute_avg(self, plan: AvgPlan):
        """
        Execute a AVG(column) query plan.
        
        Args:
            plan: AvgPlan object
            
        Returns:
            Average of column values, or list of averages if GROUP BY
        """
        rows = self._get_filtered_rows(plan.table, plan.where)
        
        # Validate column exists
        if rows and plan.column not in rows[0]:
            raise ValueError(f"Column '{plan.column}' not found in table '{plan.table}'")
        
        # Handle GROUP BY
        if plan.group_by:
            return self._execute_avg_with_group_by(rows, plan)
        
        if not rows:
            return [{"avg": 0}]
        
        total = sum(row[plan.column] for row in rows)
        return [{"avg": total / len(rows)}]
    
    def _execute_avg_with_group_by(self, rows, plan: AvgPlan):
        """Execute AVG(column) with GROUP BY (supports multiple columns)."""
        group_by = plan.group_by
        
        # Normalize to list
        if isinstance(group_by, str):
            group_by_columns = [group_by]
        else:
            group_by_columns = list(group_by)
        
        # Group rows using tuple key
        groups = {}
        for row in rows:
            key = tuple(row[col] for col in group_by_columns)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # Return averages per group
        results = []
        for key, rows_in_group in groups.items():
            total = sum(row[plan.column] for row in rows_in_group)
            avg = total / len(rows_in_group) if rows_in_group else 0
            if len(group_by_columns) == 1:
                results.append({group_by_columns[0]: key[0] if isinstance(key, tuple) else key, 'avg': avg})
            else:
                result_row = {}
                for i, col in enumerate(group_by_columns):
                    result_row[col] = key[i]
                result_row['avg'] = avg
                results.append(result_row)
        
        return results
    
    def _execute_min(self, plan: MinPlan):
        """
        Execute a MIN(column) query plan.
        
        Args:
            plan: MinPlan object
            
        Returns:
            Minimum value of column, or list of minimums if GROUP BY
        """
        rows = self._get_filtered_rows(plan.table, plan.where)
        
        # Validate column exists
        if rows and plan.column not in rows[0]:
            raise ValueError(f"Column '{plan.column}' not found in table '{plan.table}'")
        
        # Handle GROUP BY
        if plan.group_by:
            return self._execute_min_with_group_by(rows, plan)
        
        if not rows:
            return [{"min": 0}]
        
        min_value = min(row[plan.column] for row in rows)
        return [{"min": min_value}]
    
    def _execute_min_with_group_by(self, rows, plan: MinPlan):
        """Execute MIN(column) with GROUP BY (supports multiple columns)."""
        group_by = plan.group_by
        
        # Normalize to list
        if isinstance(group_by, str):
            group_by_columns = [group_by]
        else:
            group_by_columns = list(group_by)
        
        # Group rows using tuple key
        groups = {}
        for row in rows:
            key = tuple(row[col] for col in group_by_columns)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # Return minimums per group
        results = []
        for key, rows_in_group in groups.items():
            min_val = min(row[plan.column] for row in rows_in_group)
            if len(group_by_columns) == 1:
                results.append({group_by_columns[0]: key[0] if isinstance(key, tuple) else key, 'min': min_val})
            else:
                result_row = {}
                for i, col in enumerate(group_by_columns):
                    result_row[col] = key[i]
                result_row['min'] = min_val
                results.append(result_row)
        
        return results
    
    def _execute_max(self, plan: MaxPlan):
        """
        Execute a MAX(column) query plan.
        
        Args:
            plan: MaxPlan object
            
        Returns:
            Maximum value of column, or list of maximums if GROUP BY
        """
        rows = self._get_filtered_rows(plan.table, plan.where)
        
        # Validate column exists
        if rows and plan.column not in rows[0]:
            raise ValueError(f"Column '{plan.column}' not found in table '{plan.table}'")
        
        # Handle GROUP BY
        if plan.group_by:
            return self._execute_max_with_group_by(rows, plan)
        
        if not rows:
            return [{"max": 0}]
        
        max_value = max(row[plan.column] for row in rows)
        return [{"max": max_value}]
    
    def _execute_max_with_group_by(self, rows, plan: MaxPlan):
        """Execute MAX(column) with GROUP BY (supports multiple columns)."""
        group_by = plan.group_by
        
        # Normalize to list
        if isinstance(group_by, str):
            group_by_columns = [group_by]
        else:
            group_by_columns = list(group_by)
        
        # Group rows using tuple key
        groups = {}
        for row in rows:
            key = tuple(row[col] for col in group_by_columns)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # Return maximums per group
        results = []
        for key, rows_in_group in groups.items():
            max_val = max(row[plan.column] for row in rows_in_group)
            if len(group_by_columns) == 1:
                results.append({group_by_columns[0]: key[0] if isinstance(key, tuple) else key, 'max': max_val})
            else:
                result_row = {}
                for i, col in enumerate(group_by_columns):
                    result_row[col] = key[i]
                result_row['max'] = max_val
                results.append(result_row)
        
        return results
    
    # =========================================================================
    # DML execution
    # =========================================================================

    def _execute_create(self, plan: CreateTablePlan):
        self.storage.create_table(plan.table_name, plan.columns)
        return [{"result": f"Table '{plan.table_name}' created successfully"}]

    def _execute_insert(self, plan: InsertPlan):
        row_id = self.storage.insert_row(
            plan.table_name, plan.columns, plan.values
        )
        return [{"result": f"1 row inserted", "row_id": row_id}]

    def _execute_delete(self, plan: DeletePlan):
        count = self.storage.delete_rows(plan.table_name, plan.where)
        return [{"result": f"{count} row(s) deleted"}]

    def _execute_update(self, plan: UpdatePlan):
        count = self.storage.update_rows(
            plan.table_name, plan.assignments, plan.where
        )
        return [{"result": f"{count} row(s) updated"}]