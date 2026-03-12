# Query Planner Layer
# Converts AST into logical query plan

from ast_nodes import SelectQuery, Condition, LogicalCondition, NotCondition, CountQuery


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