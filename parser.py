# SQL Parser - Parser
# Parses SQL queries into AST using PLY

import ply.yacc as yacc
from lexer import SQLLexer
from ast_nodes import SelectQuery, Condition

class SQLParser:
    """SQL Parser that builds AST from tokens"""
    
    tokens = SQLLexer.tokens
    
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
        'select_stmt : SELECT select_list FROM ID'
        # SELECT columns FROM table;
        p[0] = SelectQuery(columns=p[2], table=p[4])
    
    def p_select_stmt_where(self, p):
        'select_stmt : SELECT select_list FROM ID WHERE condition'
        # SELECT columns FROM table WHERE condition;
        p[0] = SelectQuery(columns=p[2], table=p[4], where=p[6])
    
    def p_select_list_star(self, p):
        'select_list : STAR'
        p[0] = '*'
    
    def p_select_list_column_list(self, p):
        'select_list : column_list'
        p[0] = p[1]
    
    def p_column_list_single(self, p):
        'column_list : ID'
        p[0] = [p[1]]
    
    def p_column_list_multiple(self, p):
        'column_list : column_list COMMA ID'
        p[0] = p[1] + [p[3]]
    
    def p_condition(self, p):
        'condition : ID comparator value'
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
