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

class SelectQuery(ASTNode):
    """Represents a SELECT query: SELECT columns FROM table [WHERE condition] [ORDER BY column];"""
    
    def __init__(self, columns, table, where=None, order_by=None):
        # columns can be a list of column names or "*" for SELECT *
        self.columns = columns
        self.table = table
        self.where = where  # Optional Condition
        self.order_by = order_by  # Optional order by column
    
    def __repr__(self):
        return f"SelectQuery(columns={self.columns!r}, table={self.table!r}, where={self.where!r}, order_by={self.order_by!r})"