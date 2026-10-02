grammar JCL;

// The universal grammar for Marabunta's Job Control Language

script: statement* EOF;

statement
    : map_stmt
    | reduce_stmt
    | shuffle_stmt
    | define_stmt
    | given_stmt
    ;

map_stmt: 'MAP' IDENTIFIER 'PULL' 'FROM' 'STREAM' '(' arg_list ')';
reduce_stmt: 'REDUCE' 'BY' IDENTIFIER;
shuffle_stmt: 'SHUFFLE' 'WITH' STRING;
define_stmt: 'DEFINE' 'SINK' IDENTIFIER 'AS' STRING;
given_stmt: 'GIVEN' '(' arg_list ')';

arg_list: arg (',' arg)*;
arg: IDENTIFIER '=>' (STRING | NUMBER | IDENTIFIER);

// Lexer Rules
MAP: 'MAP';
PULL: 'PULL';
FROM: 'FROM';
STREAM: 'STREAM';
REDUCE: 'REDUCE';
BY: 'BY';
SHUFFLE: 'SHUFFLE';
WITH: 'WITH';
DEFINE: 'DEFINE';
SINK: 'SINK';
AS: 'AS';
GIVEN: 'GIVEN';

IDENTIFIER: [a-zA-Z_][a-zA-Z_0-9]*;
STRING: ''' ~['
]*? ''';
NUMBER: [0-9]+ ('.' [0-9]+)?;
WS: [ 	
]+ -> skip;
