# SQL Parser - Parser
# Parses SQL queries into AST using PLY

import ply.yacc as yacc
from lexer import SQLLexer
from ast_nodes import SelectQuery, Condition, LogicalCondition, NotCondition, CountQuery, SumQuery, AvgQuery, MinQuery, MaxQuery

class SQLParser:
    """SQL Parser that builds AST from tokens"""
    
    tokens = SQLLexer.tokens
    
    # Precedence: NOT > AND > OR
    precedence = (
        ('right', 'NOT'),
        ('left', 'AND'),
        ('left', 'OR'),
    )
    
    def __init__(self):
        self.lexer = SQLLexer()
        self.lexer.build()
        self.parser = yacc.yacc(module=self)
    
    def parse(self, data):
        """Parse SQL query and return AST"""
        return self.parser.parse(data, lexer=self.lexer.lexer)
    
    # Grammar rules
    
    def p_query(self, p):
        'query : select_stmt'
        p[0] = p[1]
    
    def p_select_stmt(self, p):
        'select_stmt : SELECT optional_distinct select_list FROM ID optional_where optional_group optional_having optional_order optional_limit'
        # SELECT [DISTINCT] columns FROM table [WHERE condition] [GROUP BY column] [HAVING condition] [ORDER BY column] [LIMIT number];
        # p[1]=SELECT, p[2]=optional_distinct, p[3]=select_list, p[4]=FROM, p[5]=ID, p[6]=optional_where, p[7]=optional_group, p[8]=optional_having, p[9]=optional_order, p[10]=optional_limit
        where_clause = p[6] if p[6] else None
        group_clause = p[7] if p[7] else None
        having_clause = p[8] if p[8] else None
        order_clause = p[9] if p[9] else None
        limit_clause = p[10] if p[10] else None
        distinct_flag = p[2] if p[2] else False
        p[0] = SelectQuery(columns=p[3], table=p[5], where=where_clause, order_by=order_clause, limit=limit_clause, distinct=distinct_flag, group_by=group_clause, having=having_clause)
    
    def p_select_stmt_count(self, p):
        'select_stmt : SELECT COUNT LPAREN STAR RPAREN FROM ID optional_where optional_group'
        # SELECT COUNT(*) FROM table [WHERE condition] [GROUP BY column1, column2];
        p[0] = CountQuery(table=p[7], where=p[8] if p[8] else None, group_by=p[9] if p[9] else None)
    
    def p_select_stmt_sum(self, p):
        'select_stmt : SELECT SUM LPAREN ID RPAREN FROM ID optional_where optional_group'
        # SELECT SUM(column) FROM table [WHERE condition] [GROUP BY column1, column2];
        p[0] = SumQuery(column=p[4], table=p[7], where=p[8] if p[8] else None, group_by=p[9] if p[9] else None)
    
    def p_select_stmt_avg(self, p):
        'select_stmt : SELECT AVG LPAREN ID RPAREN FROM ID optional_where optional_group'
        # SELECT AVG(column) FROM table [WHERE condition] [GROUP BY column1, column2];
        p[0] = AvgQuery(column=p[4], table=p[7], where=p[8] if p[8] else None, group_by=p[9] if p[9] else None)
    
    def p_select_stmt_min(self, p):
        'select_stmt : SELECT MIN LPAREN ID RPAREN FROM ID optional_where optional_group'
        # SELECT MIN(column) FROM table [WHERE condition] [GROUP BY column1, column2];
        p[0] = MinQuery(column=p[4], table=p[7], where=p[8] if p[8] else None, group_by=p[9] if p[9] else None)
    
    def p_select_stmt_max(self, p):
        'select_stmt : SELECT MAX LPAREN ID RPAREN FROM ID optional_where optional_group'
        # SELECT MAX(column) FROM table [WHERE condition] [GROUP BY column1, column2];
        p[0] = MaxQuery(column=p[4], table=p[7], where=p[8] if p[8] else None, group_by=p[9] if p[9] else None)
    
    def p_optional_where(self, p):
        'optional_where : WHERE condition'
        p[0] = p[2]
    
    def p_optional_where_empty(self, p):
        'optional_where : empty'
        p[0] = None
    
    def p_optional_group(self, p):
        'optional_group : GROUP BY group_by_list'
        p[0] = p[3]
    
    def p_optional_group_empty(self, p):
        'optional_group : empty'
        p[0] = None
    
    def p_group_by_list_single(self, p):
        'group_by_list : ID'
        p[0] = [p[1]]
    
    def p_group_by_list_multiple(self, p):
        'group_by_list : group_by_list COMMA ID'
        p[0] = p[1] + [p[3]]
    
    def p_optional_having(self, p):
        'optional_having : HAVING having_condition'
        p[0] = p[2]
    
    def p_optional_having_empty(self, p):
        'optional_having : empty'
        p[0] = None
    
    def p_having_condition(self, p):
        '''having_condition : aggregate_comparison'''
        p[0] = p[1]
    
    def p_aggregate_comparison(self, p):
        '''aggregate_comparison : COUNT LPAREN STAR RPAREN comparator NUMBER
                               | aggregate_func LPAREN ID RPAREN comparator NUMBER'''
        if p[1] == 'count':
            p[0] = {'type': 'aggregate', 'func': 'count', 'column': None, 'operator': p[5], 'value': p[6]}
        else:
            p[0] = {'type': 'aggregate', 'func': p[1], 'column': p[3], 'operator': p[5], 'value': p[6]}
    
    def p_optional_order(self, p):
        'optional_order : ORDER BY order_by_expr'
        p[0] = p[3]
    
    def p_optional_order_empty(self, p):
        'optional_order : empty'
        p[0] = None
    
    def p_order_by_expr_id(self, p):
        'order_by_expr : ID'
        p[0] = p[1]
    
    def p_order_by_expr_aggregate(self, p):
        'order_by_expr : COUNT LPAREN STAR RPAREN'
        p[0] = {'type': 'aggregate', 'func': 'count'}
    
    def p_order_by_expr_aggregate_column(self, p):
        'order_by_expr : aggregate_func LPAREN ID RPAREN'
        p[0] = {'type': 'aggregate', 'func': p[1], 'column': p[3]}
    
    def p_aggregate_func(self, p):
        '''aggregate_func : SUM
                          | AVG
                          | MIN
                          | MAX'''
        p[0] = p[1]
    
    def p_optional_limit(self, p):
        'optional_limit : LIMIT NUMBER'
        p[0] = p[2]
    
    def p_optional_limit_empty(self, p):
        'optional_limit : empty'
        p[0] = None
    
    def p_empty(self, p):
        'empty :'
        pass
    
    def p_optional_distinct(self, p):
        'optional_distinct : DISTINCT'
        p[0] = True
    
    def p_optional_distinct_empty(self, p):
        'optional_distinct : empty'
        p[0] = False
    
    def p_select_list_star(self, p):
        'select_list : STAR'
        p[0] = '*'
    
    def p_select_list_column_list(self, p):
        'select_list : column_list'
        p[0] = p[1]
    
    def p_column_list_single(self, p):
        'column_list : column_item'
        p[0] = [p[1]]
    
    def p_column_list_multiple(self, p):
        'column_list : column_list COMMA column_item'
        p[0] = p[1] + [p[3]]
    
    def p_column_item_id(self, p):
        'column_item : ID'
        p[0] = {'type': 'column', 'name': p[1]}
    
    def p_column_item_count(self, p):
        'column_item : COUNT LPAREN STAR RPAREN'
        p[0] = {'type': 'aggregate', 'func': 'count'}
    
    def p_column_item_sum(self, p):
        'column_item : SUM LPAREN ID RPAREN'
        p[0] = {'type': 'aggregate', 'func': 'sum', 'column': p[3]}
    
    def p_column_item_avg(self, p):
        'column_item : AVG LPAREN ID RPAREN'
        p[0] = {'type': 'aggregate', 'func': 'avg', 'column': p[3]}
    
    def p_column_item_min(self, p):
        'column_item : MIN LPAREN ID RPAREN'
        p[0] = {'type': 'aggregate', 'func': 'min', 'column': p[3]}
    
    def p_column_item_max(self, p):
        'column_item : MAX LPAREN ID RPAREN'
        p[0] = {'type': 'aggregate', 'func': 'max', 'column': p[3]}
    
    def p_condition(self, p):
        '''condition : or_condition'''
        p[0] = p[1]
    
    def p_or_condition(self, p):
        '''or_condition : and_condition
                        | or_condition OR and_condition'''
        if len(p) == 2:
            p[0] = p[1]
        else:
            p[0] = LogicalCondition(left=p[1], operator=p[2], right=p[3])
    
    def p_and_condition(self, p):
        '''and_condition : not_condition
                         | and_condition AND not_condition'''
        if len(p) == 2:
            p[0] = p[1]
        else:
            p[0] = LogicalCondition(left=p[1], operator=p[2], right=p[3])
    
    def p_not_condition(self, p):
        '''not_condition : simple_condition
                         | NOT simple_condition'''
        if len(p) == 2:
            p[0] = p[1]
        else:
            p[0] = NotCondition(condition=p[2])
    
    def p_simple_condition(self, p):
        'simple_condition : ID comparator value'
        p[0] = Condition(column=p[1], operator=p[2], value=p[3])
    
    def p_comparator(self, p):
        '''comparator : EQUAL
                      | GT
                      | LT
                      | GE
                      | LE'''
        p[0] = p[1]
    
    def p_value_number(self, p):
        'value : NUMBER'
        p[0] = p[1]
    
    def p_value_string(self, p):
        'value : STRING'
        p[0] = p[1]
    
    # Error handling
    def p_error(self, p):
        if p:
            print(f"Syntax error at '{p.value}'")
        else:
            print("Syntax error at EOF")


def get_parser():
    """Factory function to create and return a parser instance"""
    return SQLParser()
