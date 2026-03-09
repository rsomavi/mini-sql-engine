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

class SelectQuery(ASTNode):
    """Represents a SELECT query: SELECT columns FROM table [WHERE condition];"""
    
    def __init__(self, columns, table, where=None):
        # columns can be a list of column names or "*" for SELECT *
        self.columns = columns
        self.table = table
        self.where = where  # Optional Condition
    
    def __repr__(self):
        return f"SelectQuery(columns={self.columns!r}, table={self.table!r}, where={self.where!r})"