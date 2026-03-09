# SQL Parser - AST Nodes
# Minimal AST for educational purposes

class ASTNode:
    """Base class for all AST nodes"""
    pass

class SelectQuery(ASTNode):
    """Represents a SELECT query: SELECT column FROM table;"""
    
    def __init__(self, column: str, table: str):
        self.column = column
        self.table = table
    
    def __repr__(self):
        return f"SelectQuery(column={self.column!r}, table={self.table!r})"