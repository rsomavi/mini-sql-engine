# SQL Parser - Parser
# Parses SQL queries into AST using PLY

import ply.yacc as yacc
from lexer import SQLLexer
from ast_nodes import SelectQuery

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
        'select_stmt : SELECT ID FROM ID'
        # SELECT column FROM table;
        p[0] = SelectQuery(column=p[2], table=p[4])
    
    # Error handling
    def p_error(self, p):
        if p:
            print(f"Syntax error at '{p.value}'")
        else:
            print("Syntax error at EOF")


def get_parser():
    """Factory function to create and return a parser instance"""
    return SQLParser()
