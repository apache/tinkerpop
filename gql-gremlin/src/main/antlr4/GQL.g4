/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Minimal GQL grammar covering the MATCH subset for node/edge patterns.
 *
 * Supported patterns:
 *   - Anonymous nodes:              ()
 *   - Variable-only nodes:          (n)
 *   - Labeled nodes:                (:Label) or (n:Label)
 *   - Directed edges:               -[e:Label]->  or  -[:Label]->  or  -[e]->  or  -[]->
 *   - Reverse directed edges:       <-[e:Label]-  or  <-[:Label]-  or  <-[e]-  or  <-[]-
 *   - Undirected edges:             -[e:Label]-   or  -[:Label]-   or  -[e]-   or  -[]-
 *   - Multiple comma-separated path patterns in a single MATCH clause
 *   - Inline property filters on nodes: (n:Label {key: 'value', count: 42i, flag: true, x: $param})
 *
 * Property value literal types align with Gremlin's type system:
 *   Strings:  single-quoted 'text' or double-quoted "text", Java escape sequences supported
 *   Integers: optional sign, decimal digits, optional suffix (b/B=Byte, s/S=Short, i/I=Integer,
 *             l/L=Long, n/N=BigInteger); no suffix defaults to smallest fitting type
 *   Floats:   decimal-point form or integer-form with suffix (f/F=Float, d/D=Double, m/M=BigDecimal);
 *             no suffix defaults to Double
 *   Booleans: true/false (case-insensitive)
 *   Null:     null
 *   Special:  NaN, Infinity, +Infinity, -Infinity
 *   Params:   $name (resolved from the params map at execution time)
 *
 * Out of scope: WHERE clause, RETURN, path quantifiers.
 */
grammar GQL;

// ─── Parser Rules ────────────────────────────────────────────────────────────

/**
 * Top-level entry point: a single MATCH clause followed by end-of-input.
 */
matchClause
    : K_MATCH graphPattern EOF
    ;

/**
 * A graph pattern is one or more comma-separated path patterns.
 *
 * Example: MATCH (n:Person)-[:KNOWS]->(m), (p:Movie)
 */
graphPattern
    : pathPattern (COMMA pathPattern)*
    ;

/**
 * A path pattern is a node pattern optionally extended by alternating
 * edge and node patterns.
 *
 * Example: (n:Person)-[:KNOWS]->(m:Person)-[:LIKES]->(c)
 */
pathPattern
    : nodePattern (edgePattern nodePattern)*
    ;

/**
 * A node pattern: parenthesised element with optional variable, label, and property filter.
 *
 * Examples: ()  (n)  (:Person)  (n:Person)  (n:Person {name: 'Alice'})  (n {age: $age})
 */
nodePattern
    : LPAREN elementPatternFiller RPAREN
    ;

/**
 * Shared inner content for node patterns:
 * an optional variable name, an optional label, and an optional property filter map.
 *
 * Examples: (empty)  n  :Label  n:Label  n:Label {key: value}
 */
elementPatternFiller
    : elementVariable? labelSpec? propertyFilter?
    ;

/**
 * A single colon-prefixed label.
 *
 * Example: :KNOWS
 */
labelSpec
    : COLON labelName
    ;

/**
 * An inline property filter map: a comma-separated list of key-value pairs
 * enclosed in curly braces.
 *
 * Example: {name: 'Alice', age: 30i, active: true, score: $minScore}
 */
propertyFilter
    : LBRACE propertyPair (COMMA propertyPair)* RBRACE
    ;

/**
 * A single key-value predicate within a property filter.
 *
 * Example: name: 'Alice'
 */
propertyPair
    : propertyKey COLON propertyValue
    ;

/**
 * The property key (always an identifier).
 */
propertyKey
    : IDENTIFIER
    ;

/**
 * A property value: either a literal or a parameter reference.
 */
propertyValue
    : literal
    | paramRef
    ;

/**
 * Literal value types, aligned with Gremlin's GenericLiteralVisitor type system.
 * FLOAT_LITERAL must precede INTEGER_LITERAL in ANTLR alternatives so that the
 * lexer-level maximal-munch already resolved the token type correctly.
 */
literal
    : STRING_LITERAL
    | FLOAT_LITERAL
    | INTEGER_LITERAL
    | K_TRUE
    | K_FALSE
    | K_NULL
    | K_NAN
    | SIGNED_INFINITY
    | K_INFINITY
    ;

/**
 * A parameter reference: a dollar sign followed by an identifier.
 *
 * Example: $personName
 */
paramRef
    : DOLLAR IDENTIFIER
    ;

/**
 * Three edge pattern flavors, all requiring bracket notation so that
 * variable binding and label are available.
 */
edgePattern
    : directedEdge
    | reverseDirectedEdge
    | undirectedEdge
    ;

/**
 * Directed edge:  -[var?:Label?]->
 *
 * Example: -[e:KNOWS]->
 */
directedEdge
    : DASH LBRACKET elementPatternFiller RBRACKET ARROW
    ;

/**
 * Reverse directed edge:  <-[var?:Label?]-
 *
 * Example: <-[e:KNOWS]-
 */
reverseDirectedEdge
    : LARROW LBRACKET elementPatternFiller RBRACKET DASH
    ;

/**
 * Undirected edge:  -[var?:Label?]-
 *
 * Example: -[e:KNOWS]-
 */
undirectedEdge
    : DASH LBRACKET elementPatternFiller RBRACKET DASH
    ;

/**
 * A variable name bound to a node or edge element.
 */
elementVariable
    : IDENTIFIER
    ;

/**
 * A label name applied to a node or edge element.
 */
labelName
    : IDENTIFIER
    ;

// ─── Lexer Rules ─────────────────────────────────────────────────────────────

/**
 * Keywords — must be declared before IDENTIFIER so they take precedence
 * when the input matches both.
 * K_MATCH stays case-insensitive (ISO GQL keyword convention).
 * Numeric special-value keywords use exact case to match Gremlin's grammar.
 */
K_MATCH    : [Mm][Aa][Tt][Cc][Hh] ;
K_TRUE     : [Tt][Rr][Uu][Ee] ;
K_FALSE    : [Ff][Aa][Ll][Ss][Ee] ;
K_NULL     : 'null' ;
K_NAN      : 'NaN' ;
K_INFINITY : 'Infinity' ;

/**
 * Two-character operators must be declared before the single DASH token
 * so that ANTLR4's maximal-munch rule chooses the longer match.
 */
ARROW  : '->' ;   // directed edge tail
LARROW : '<-' ;   // reverse directed edge head

/**
 * Single-character punctuation.
 */
LPAREN   : '(' ;
RPAREN   : ')' ;
LBRACKET : '[' ;
RBRACKET : ']' ;
LBRACE   : '{' ;
RBRACE   : '}' ;
DASH     : '-' ;
COLON    : ':' ;
COMMA    : ',' ;
DOLLAR   : '$' ;

/**
 * Signed infinity: +Infinity or -Infinity.
 * Declared after ARROW/LARROW so that -> and <- are preferred, and before
 * INTEGER_LITERAL/FLOAT_LITERAL so the sign is consumed as part of this token.
 * Maximal-munch selects this 9-char token over DASH(1) + K_INFINITY(8).
 */
SIGNED_INFINITY : [+-] 'Infinity' ;

/**
 * String literal: single-quoted or double-quoted, supporting Java-style escape sequences.
 * Escape sequences: \b \t \n \f \r \" \' \\ plus octal and unicode (\uXXXX).
 */
STRING_LITERAL
    : '\'' SqStringChar* '\''
    | '"'  DqStringChar* '"'
    ;

fragment SqStringChar : ~['\\\r\n] | EscapeSeq ;
fragment DqStringChar : ~["\\\r\n] | EscapeSeq ;

fragment EscapeSeq
    : '\\' [btnfr"'\\]
    | '\\' [0-7] ([0-7] [0-7]?)?
    | '\\' 'u' HexDigit HexDigit HexDigit HexDigit
    ;

fragment HexDigit : [0-9a-fA-F] ;

/**
 * Float literal: decimal-point form or integer-digits with float type suffix.
 * Must be declared before INTEGER_LITERAL so that ANTLR's maximal-munch rule
 * picks the longer token for "3.14" and "29f" over just "3" and "29".
 *
 * Type suffix (case-insensitive): f/F=Float, d/D=Double, m/M=BigDecimal.
 * No suffix on decimal-point form defaults to Double.
 * Optional leading sign (+/-).
 */
FLOAT_LITERAL
    : [+-]? [0-9]+ '.' [0-9]* [fFdDmM]?   // decimal form: 3.14  -1.5f  2.
    | [+-]? [0-9]+             [fFdDmM]     // integer-digits + float suffix: 29f  -1d
    ;

/**
 * Integer literal: optional sign, decimal digits, optional type suffix.
 * Mirrors Gremlin's IntegerTypeSuffix: b/B=Byte, s/S=Short, i/I=Integer,
 * l/L=Long, n/N=BigInteger.  No suffix: smallest fitting type
 * (Integer first, then Long, then BigInteger — matching Gremlin's default).
 */
INTEGER_LITERAL : [+-]? [0-9]+ [bBsSnNiIlL]? ;

/**
 * Identifiers: used for both variable names and label names.
 * Must start with a letter or underscore, followed by zero or more
 * letters, digits, or underscores.
 */
IDENTIFIER : [a-zA-Z_][a-zA-Z_0-9]* ;

/**
 * Whitespace is silently discarded.
 */
WS : [ \t\r\n]+ -> skip ;
