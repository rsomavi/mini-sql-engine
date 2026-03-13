# SQL Parser - AST Nodes
# Minimal AST for educational purposes

class ASTNode:
    """Base class for all AST nodes"""
    pass

class Condition(ASTNode):
    """Represents a WHERE condition: column operator value"""
    
    def __init__(self, column, operator, value):
        self.column = column
        self.operator = operator
        self.value = value
    
    def __repr__(self):
        return f"Condition(column={self.column!r}, operator={self.operator!r}, value={self.value!r})"

class LogicalCondition(ASTNode):
    """Represents a logical condition: left AND/OR right"""
    
    def __init__(self, left, operator, right):
        self.left = left
        self.operator = operator
        self.right = right
    
    def __repr__(self):
        return f"LogicalCondition(left={self.left!r}, operator={self.operator!r}, right={self.right!r})"

class NotCondition(ASTNode):
    """Represents a negated condition: NOT condition"""
    
    def __init__(self, condition):
        self.condition = condition
    
    def __repr__(self):
        return f"NotCondition(condition={self.condition!r})"

class CountQuery(ASTNode):
    """Represents a COUNT(*) query: SELECT COUNT(*) FROM table [WHERE condition]"""
    
    def __init__(self, table, where=None):
        self.table = table
        self.where = where  # Optional Condition
    
    def __repr__(self):
        return f"CountQuery(table={self.table!r}, where={self.where!r})"

class SumQuery(ASTNode):
    """Represents a SUM(column) query: SELECT SUM(column) FROM table [WHERE condition]"""
    
    def __init__(self, column, table, where=None):
        self.column = column
        self.table = table
        self.where = where  # Optional Condition
    
    def __repr__(self):
        return f"SumQuery(column={self.column!r}, table={self.table!r}, where={self.where!r})"

class AvgQuery(ASTNode):
    """Represents a AVG(column) query: SELECT AVG(column) FROM table [WHERE condition]"""
    
    def __init__(self, column, table, where=None):
        self.column = column
        self.table = table
        self.where = where  # Optional Condition
    
    def __repr__(self):
        return f"AvgQuery(column={self.column!r}, table={self.table!r}, where={self.where!r})"

class MinQuery(ASTNode):
    """Represents a MIN(column) query: SELECT MIN(column) FROM table [WHERE condition]"""
    
    def __init__(self, column, table, where=None):
        self.column = column
        self.table = table
        self.where = where  # Optional Condition
    
    def __repr__(self):
        return f"MinQuery(column={self.column!r}, table={self.table!r}, where={self.where!r})"

class MaxQuery(ASTNode):
    """Represents a MAX(column) query: SELECT MAX(column) FROM table [WHERE condition]"""
    
    def __init__(self, column, table, where=None):
        self.column = column
        self.table = table
        self.where = where  # Optional Condition
    
    def __repr__(self):
        return f"MaxQuery(column={self.column!r}, table={self.table!r}, where={self.where!r})"

class SelectQuery(ASTNode):
    """Represents a SELECT query: SELECT [DISTINCT] columns FROM table [WHERE condition] [ORDER BY column] [LIMIT number];"""
    
    def __init__(self, columns, table, where=None, order_by=None, limit=None, distinct=False):
        # columns can be a list of column names, "*" for SELECT *, or "COUNT" for COUNT(*)
        self.columns = columns
        self.table = table
        self.where = where  # Optional Condition
        self.order_by = order_by  # Optional order by column
        self.limit = limit  # Optional limit value
        self.distinct = distinct  # Whether DISTINCT is specified
    
    def __repr__(self):
        return f"SelectQuery(columns={self.columns!r}, table={self.table!r}, where={self.where!r}, order_by={self.order_by!r}, limit={self.limit!r}, distinct={self.distinct!r})"