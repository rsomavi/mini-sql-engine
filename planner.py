# Query Planner Layer
# Converts AST into logical query plan

from ast_nodes import SelectQuery, Condition, LogicalCondition, NotCondition, CountQuery, SumQuery, AvgQuery, MinQuery, MaxQuery


class SelectPlan:
    """Logical plan node for SELECT queries."""
    
    def __init__(self, columns, table, where, order_by=None, limit=None, distinct=False):
        self.columns = columns
        self.table = table
        self.where = where
        self.order_by = order_by
        self.limit = limit
        self.distinct = distinct


class CountPlan:
    """Logical plan node for COUNT(*) queries."""
    
    def __init__(self, table, where):
        self.table = table
        self.where = where


class SumPlan:
    """Logical plan node for SUM(column) queries."""
    
    def __init__(self, column, table, where):
        self.column = column
        self.table = table
        self.where = where


class AvgPlan:
    """Logical plan node for AVG(column) queries."""
    
    def __init__(self, column, table, where):
        self.column = column
        self.table = table
        self.where = where


class MinPlan:
    """Logical plan node for MIN(column) queries."""
    
    def __init__(self, column, table, where):
        self.column = column
        self.table = table
        self.where = where


class MaxPlan:
    """Logical plan node for MAX(column) queries."""
    
    def __init__(self, column, table, where):
        self.column = column
        self.table = table
        self.where = where


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
        return SelectPlan(
            columns=select_node.columns,
            table=select_node.table,
            where=select_node.where,
            order_by=select_node.order_by,
            limit=select_node.limit,
            distinct=getattr(select_node, 'distinct', False)
        )
    
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
            where=count_node.where
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
            where=sum_node.where
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
            where=avg_node.where
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
            where=min_node.where
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
            where=max_node.where
        )