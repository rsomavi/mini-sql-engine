# SQL Parser - Lexer
# Tokenizes SQL queries using PLY

import ply.lex as lex

class SQLLexer:
    """SQL Lexer that tokenizes SQL queries"""
    
    # List of token names
    tokens = (
        'SELECT',
        'FROM',
        'ID',
    )
    
    # Reserved words
    reserved = {
        'SELECT': 'SELECT',
        'FROM': 'FROM',
    }
    
    # Regular expression rules for simple tokens
    t_SELECT = r'SELECT'
    t_FROM = r'FROM'
    
    # Identifier (table/column names)
    def t_ID(self, t):
        r'[a-zA-Z_][a-zA-Z0-9_]*'
        t.type = self.reserved.get(t.value, 'ID')  # Check for reserved words
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