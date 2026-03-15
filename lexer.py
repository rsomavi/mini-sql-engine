# SQL Parser - Lexer
# Tokenizes SQL queries using PLY

import ply.lex as lex
import re

class SQLLexer:
    """SQL Lexer that tokenizes SQL queries"""
    
    # List of token names
    tokens = (
        'SELECT',
        'FROM',
        'WHERE',
        'ORDER',
        'BY',
        'LIMIT',
        'COUNT',
        'SUM',
        'AVG',
        'MIN',
        'MAX',
        'DISTINCT',
        'GROUP',
        'ID',
        'STAR',
        'COMMA',
        'EQUAL',
        'GT',
        'LT',
        'GE',
        'LE',
        'NUMBER',
        'STRING',
        'AND',
        'OR',
        'NOT',
        'LPAREN',
        'RPAREN',
    )
    
    # Reserved words
    reserved = {
        'SELECT': 'SELECT',
        'FROM': 'FROM',
        'WHERE': 'WHERE',
        'ORDER': 'ORDER',
        'BY': 'BY',
        'LIMIT': 'LIMIT',
        'COUNT': 'COUNT',
        'SUM': 'SUM',
        'AVG': 'AVG',
        'MIN': 'MIN',
        'MAX': 'MAX',
        'DISTINCT': 'DISTINCT',
        'GROUP': 'GROUP',
        'AND': 'AND',
        'OR': 'OR',
        'NOT': 'NOT',
    }
    
    # Regular expression rules for simple tokens
    # Note: GE and LE must be defined before GT and LT to avoid incorrect matching
    # Using (?i) flag makes patterns case-insensitive
    t_SELECT = r'(?i)SELECT'
    t_FROM = r'(?i)FROM'
    t_WHERE = r'(?i)WHERE'
    t_ORDER = r'(?i)ORDER'
    t_BY = r'(?i)BY'
    t_LIMIT = r'(?i)LIMIT'
    t_COUNT = r'(?i)COUNT'
    t_SUM = r'(?i)SUM'
    t_AVG = r'(?i)AVG'
    t_MIN = r'(?i)MIN'
    t_MAX = r'(?i)MAX'
    t_DISTINCT = r'(?i)DISTINCT'
    t_GROUP = r'(?i)GROUP'
    t_STAR = r'\*'
    t_LPAREN = r'\('
    t_RPAREN = r'\)'
    t_COMMA = r','
    t_EQUAL = r'='
    t_GE = r'>='
    t_LE = r'<='
    t_GT = r'>'
    t_LT = r'<'
    t_AND = r'(?i)AND'
    t_OR = r'(?i)OR'
    t_NOT = r'(?i)NOT'
    
    # Number (integer)
    def t_NUMBER(self, t):
        r'\d+'
        t.value = int(t.value)
        return t
    
    # String (double or single quoted)
    def t_STRING(self, t):
        r'("([^"]*)")|(\'([^\']*)\')'
        # Remove both types of quotes
        if t.value.startswith('"'):
            t.value = t.value[1:-1]
        else:
            t.value = t.value[1:-1]
        return t
    
    # Identifier (table/column names)
    def t_ID(self, t):
        r'[a-zA-Z_][a-zA-Z0-9_]*'
        # Convert to uppercase for reserved words, lowercase for IDs
        t.value = t.value.lower()
        t.type = self.reserved.get(t.value.upper(), 'ID')  # Check for reserved words
        return t
    
    # Ignore whitespace
    t_ignore = ' \t'
    
    # Ignore comments (optional)
    def t_comment(self, t):
        r'--.*'
        pass
    
    # Track line numbers
    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)
    
    # Error handling
    def t_error(self, t):
        print(f"Illegal character '{t.value[0]}' at line {t.lexer.lineno}")
        t.lexer.skip(1)
    
    # Build the lexer
    def build(self, **kwargs):
        self.lexer = lex.lex(module=self, **kwargs)
    
    # Test the lexer
    def test(self, data):
        self.lexer.input(data)
        while True:
            tok = self.lexer.token()
            if not tok:
                break
            print(tok)


def get_lexer():
    """Factory function to create and return a lexer instance"""
    lexer = SQLLexer()
    lexer.build()
    return lexer