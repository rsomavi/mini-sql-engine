# Query Planner Layer
# Converts AST into logical query plan

from ast_nodes import (SelectQuery, Condition, LogicalCondition, NotCondition,
                       CountQuery, SumQuery, AvgQuery, MinQuery, MaxQuery,
                       CreateTableQuery, InsertQuery, DeleteQuery, UpdateQuery)

class SelectPlan:
    """Logical plan node for SELECT queries."""
    
    def __init__(self, columns, table, where, order_by=None, limit=None, distinct=False, group_by=None, having=None, join_table=None, join_condition=None):
        self.columns = columns
        self.table = table
        self.where = where
        self.order_by = order_by
        self.limit = limit
        self.distinct = distinct
        self.group_by = group_by
        self.having = having
        self.join_table = join_table
        self.join_condition = join_condition


class CountPlan:
    """Logical plan node for COUNT(*) queries."""
    
    def __init__(self, table, where, group_by=None):
        self.table = table
        self.where = where
        self.group_by = group_by


class SumPlan:
    """Logical plan node for SUM(column) queries."""
    
    def __init__(self, column, table, where, group_by=None):
        self.column = column
        self.table = table
        self.where = where
        self.group_by = group_by


class AvgPlan:
    """Logical plan node for AVG(column) queries."""
    
    def __init__(self, column, table, where, group_by=None):
        self.column = column
        self.table = table
        self.where = where
        self.group_by = group_by


class MinPlan:
    """Logical plan node for MIN(column) queries."""
    
    def __init__(self, column, table, where, group_by=None):
        self.column = column
        self.table = table
        self.where = where
        self.group_by = group_by


class MaxPlan:
    """Logical plan node for MAX(column) queries."""
    
    def __init__(self, column, table, where, group_by=None):
        self.column = column
        self.table = table
        self.where = where
        self.group_by = group_by


class QueryPlanner:
    """Converts AST nodes into query plans."""
    
    def plan(self, ast):
        """
        Convert an AST node into a query plan.
        
        Args:
            ast: AST node (SelectQuery, etc.)
            
        Returns:
            Query plan object (SelectPlan, etc.)
        """
        if isinstance(ast, SelectQuery):
            return self._plan_select(ast)
        elif isinstance(ast, CountQuery):
            return self._plan_count(ast)
        elif isinstance(ast, SumQuery):
            return self._plan_sum(ast)
        elif isinstance(ast, AvgQuery):
            return self._plan_avg(ast)
        elif isinstance(ast, MinQuery):
            return self._plan_min(ast)
        elif isinstance(ast, MaxQuery):
            return self._plan_max(ast)
        elif isinstance(ast, CreateTableQuery):
            return self._plan_create(ast)
        elif isinstance(ast, InsertQuery):
            return self._plan_insert(ast)
        elif isinstance(ast, DeleteQuery):
            return self._plan_delete(ast)
        elif isinstance(ast, UpdateQuery):
            return self._plan_update(ast)
        else:
            raise ValueError(f"Unsupported AST node: {type(ast).__name__}")
    
    def _plan_select(self, select_node: SelectQuery):
        """
        Convert a SelectQuery AST into a SelectPlan.
        
        Args:
            select_node: SelectQuery AST node
            
        Returns:
            SelectPlan object
        """
        # Extract aggregate column info if present
        sum_column = None
        avg_column = None
        min_column = None
        max_column = None
        
        columns = select_node.columns
        if isinstance(columns, list):
            for col in columns:
                if isinstance(col, dict):
                    if col.get('type') == 'aggregate':
                        func = col.get('func')
                        col_name = col.get('column')
                        if func == 'sum':
                            sum_column = col_name
                        elif func == 'avg':
                            avg_column = col_name
                        elif func == 'min':
                            min_column = col_name
                        elif func == 'max':
                            max_column = col_name
        
        plan = SelectPlan(
            columns=select_node.columns,
            table=select_node.table,
            where=select_node.where,
            order_by=select_node.order_by,
            limit=select_node.limit,
            distinct=getattr(select_node, 'distinct', False),
            group_by=getattr(select_node, 'group_by', None),
            having=getattr(select_node, 'having', None),
            join_table=getattr(select_node, 'join_table', None),
            join_condition=getattr(select_node, 'join_condition', None)
        )
        # Store aggregate column info on the plan
        plan.sum_column = sum_column
        plan.avg_column = avg_column
        plan.min_column = min_column
        plan.max_column = max_column
        
        return plan
    
    def _plan_count(self, count_node: CountQuery):
        """
        Convert a CountQuery AST into a CountPlan.
        
        Args:
            count_node: CountQuery AST node
            
        Returns:
            CountPlan object
        """
        return CountPlan(
            table=count_node.table,
            where=count_node.where,
            group_by=getattr(count_node, 'group_by', None)
        )
    
    def _plan_sum(self, sum_node: SumQuery):
        """
        Convert a SumQuery AST into a SumPlan.
        
        Args:
            sum_node: SumQuery AST node
            
        Returns:
            SumPlan object
        """
        return SumPlan(
            column=sum_node.column,
            table=sum_node.table,
            where=sum_node.where,
            group_by=getattr(sum_node, 'group_by', None)
        )
    
    def _plan_avg(self, avg_node: AvgQuery):
        """
        Convert an AvgQuery AST into an AvgPlan.
        
        Args:
            avg_node: AvgQuery AST node
            
        Returns:
            AvgPlan object
        """
        return AvgPlan(
            column=avg_node.column,
            table=avg_node.table,
            where=avg_node.where,
            group_by=getattr(avg_node, 'group_by', None)
        )
    
    def _plan_min(self, min_node: MinQuery):
        """
        Convert a MinQuery AST into a MinPlan.
        
        Args:
            min_node: MinQuery AST node
            
        Returns:
            MinPlan object
        """
        return MinPlan(
            column=min_node.column,
            table=min_node.table,
            where=min_node.where,
            group_by=getattr(min_node, 'group_by', None)
        )
    
    def _plan_max(self, max_node: MaxQuery):
        """
        Convert a MaxQuery AST into a MaxPlan.
        
        Args:
            max_node: MaxQuery AST node
            
        Returns:
            MaxPlan object
        """
        return MaxPlan(
            column=max_node.column,
            table=max_node.table,
            where=max_node.where,
            group_by=getattr(max_node, 'group_by', None)
        )
    
    def _plan_create(self, ast):
        return CreateTablePlan(ast.table_name, ast.columns)

    def _plan_insert(self, ast):
        return InsertPlan(ast.table_name, ast.columns, ast.values)

    def _plan_delete(self, ast):
        return DeletePlan(ast.table_name, ast.where)

    def _plan_update(self, ast):
        return UpdatePlan(ast.table_name, ast.assignments, ast.where)
    
class CreateTablePlan:
    """Plan for CREATE TABLE."""
    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns    = columns  # list of ColumnDef


class InsertPlan:
    """Plan for INSERT INTO."""
    def __init__(self, table_name, columns, values):
        self.table_name = table_name
        self.columns    = columns
        self.values     = values


class DeletePlan:
    """Plan for DELETE FROM."""
    def __init__(self, table_name, where=None):
        self.table_name = table_name
        self.where      = where


class UpdatePlan:
    """Plan for UPDATE SET."""
    def __init__(self, table_name, assignments, where=None):
        self.table_name  = table_name
        self.assignments = assignments
        self.where       = where