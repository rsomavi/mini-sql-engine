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
    """Represents a COUNT(*) query: SELECT COUNT(*) FROM table [WHERE condition] [GROUP BY column1, column2]"""
    
    def __init__(self, table, where=None, group_by=None):
        self.table = table
        self.where = where  # Optional Condition
        self.group_by = group_by  # Optional group by columns (list or single column)
    
    def __repr__(self):
        return f"CountQuery(table={self.table!r}, where={self.where!r}, group_by={self.group_by!r})"

class SumQuery(ASTNode):
    """Represents a SUM(column) query: SELECT SUM(column) FROM table [WHERE condition] [GROUP BY column1, column2]"""
    
    def __init__(self, column, table, where=None, group_by=None):
        self.column = column
        self.table = table
        self.where = where  # Optional Condition
        self.group_by = group_by  # Optional group by columns (list or single column)
    
    def __repr__(self):
        return f"SumQuery(column={self.column!r}, table={self.table!r}, where={self.where!r}, group_by={self.group_by!r})"

class AvgQuery(ASTNode):
    """Represents a AVG(column) query: SELECT AVG(column) FROM table [WHERE condition] [GROUP BY column1, column2]"""
    
    def __init__(self, column, table, where=None, group_by=None):
        self.column = column
        self.table = table
        self.where = where  # Optional Condition
        self.group_by = group_by  # Optional group by columns (list or single column)
    
    def __repr__(self):
        return f"AvgQuery(column={self.column!r}, table={self.table!r}, where={self.where!r}, group_by={self.group_by!r})"

class MinQuery(ASTNode):
    """Represents a MIN(column) query: SELECT MIN(column) FROM table [WHERE condition] [GROUP BY column1, column2]"""
    
    def __init__(self, column, table, where=None, group_by=None):
        self.column = column
        self.table = table
        self.where = where  # Optional Condition
        self.group_by = group_by  # Optional group by columns (list or single column)
    
    def __repr__(self):
        return f"MinQuery(column={self.column!r}, table={self.table!r}, where={self.where!r}, group_by={self.group_by!r})"

class MaxQuery(ASTNode):
    """Represents a MAX(column) query: SELECT MAX(column) FROM table [WHERE condition] [GROUP BY column1, column2]"""
    
    def __init__(self, column, table, where=None, group_by=None):
        self.column = column
        self.table = table
        self.where = where  # Optional Condition
        self.group_by = group_by  # Optional group by columns (list or single column)
    
    def __repr__(self):
        return f"MaxQuery(column={self.column!r}, table={self.table!r}, where={self.where!r}, group_by={self.group_by!r})"

class SelectQuery(ASTNode):
    """Represents a SELECT query: SELECT [DISTINCT] columns FROM table [JOIN table2 ON condition] [WHERE condition] [GROUP BY column1, column2] [HAVING condition] [ORDER BY column] [LIMIT number];"""
    
    def __init__(self, columns, table, where=None, order_by=None, limit=None, distinct=False, group_by=None, having=None, join_table=None, join_condition=None):
        # columns can be a list of column names, "*" for SELECT *, or "COUNT" for COUNT(*)
        self.columns = columns
        self.table = table
        self.where = where  # Optional Condition
        self.order_by = order_by  # Optional order by column
        self.limit = limit  # Optional limit value
        self.distinct = distinct  # Whether DISTINCT is specified
        self.group_by = group_by  # Optional group by columns (list or single column)
        self.having = having  # Optional HAVING condition
        self.join_table = join_table  # Optional join table name
        self.join_condition = join_condition  # Optional join condition (tuple: (left_col, right_col))
    
    def __repr__(self):
        return f"SelectQuery(columns={self.columns!r}, table={self.table!r}, where={self.where!r}, order_by={self.order_by!r}, limit={self.limit!r}, distinct={self.distinct!r}, group_by={self.group_by!r}, join_table={self.join_table!r}, join_condition={self.join_condition!r})"
    
class ColumnDef(ASTNode):
    """Represents a column definition in CREATE TABLE.
    
    col_type: "INT", "FLOAT", "BOOL", "VARCHAR"
    max_size: for VARCHAR(N) — the N. 0 for fixed types.
    nullable: True if column accepts NULL.
    primary_key: True if column is PRIMARY KEY.
    """
    def __init__(self, name, col_type, max_size=0,
                 nullable=True, primary_key=False):
        self.name        = name
        self.col_type    = col_type
        self.max_size    = max_size
        self.nullable    = nullable
        self.primary_key = primary_key

    def __repr__(self):
        return (f"ColumnDef(name={self.name!r}, type={self.col_type!r}, "
                f"max_size={self.max_size}, nullable={self.nullable}, "
                f"pk={self.primary_key})")


class CreateTableQuery(ASTNode):
    """Represents: CREATE TABLE name (col type [PRIMARY KEY] [NOT NULL], ...)"""

    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns    = columns    # list of ColumnDef

    def __repr__(self):
        return (f"CreateTableQuery(table={self.table_name!r}, "
                f"columns={self.columns!r})")


class InsertQuery(ASTNode):
    """Represents: INSERT INTO table (col1, col2, ...) VALUES (v1, v2, ...)"""

    def __init__(self, table_name, columns, values):
        self.table_name = table_name
        self.columns    = columns   # list of column names
        self.values     = values    # list of values (same order as columns)

    def __repr__(self):
        return (f"InsertQuery(table={self.table_name!r}, "
                f"columns={self.columns!r}, values={self.values!r})")


class DeleteQuery(ASTNode):
    """Represents: DELETE FROM table WHERE condition"""

    def __init__(self, table_name, where=None):
        self.table_name = table_name
        self.where      = where     # optional Condition

    def __repr__(self):
        return (f"DeleteQuery(table={self.table_name!r}, "
                f"where={self.where!r})")


class UpdateQuery(ASTNode):
    """Represents: UPDATE table SET col1=val1, col2=val2 WHERE condition"""

    def __init__(self, table_name, assignments, where=None):
        self.table_name  = table_name
        self.assignments = assignments  # list of (column, value) tuples
        self.where       = where        # optional Condition

    def __repr__(self):
        return (f"UpdateQuery(table={self.table_name!r}, "
                f"assignments={self.assignments!r}, where={self.where!r})")