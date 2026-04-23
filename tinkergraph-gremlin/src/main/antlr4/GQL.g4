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
 *   - Inline property filters on nodes: (n:Label {key: 'value', count: 42, flag: true, x: $param})
 *
 * Supported property value types: string literals (single-quoted), integer literals,
 * float literals, boolean literals (true/false), and parameter references ($name).
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
 * Example: {name: 'Alice', age: 30, active: true, score: $minScore}
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
 * Literal value types: string, integer, float, or boolean.
 */
literal
    : STRING_LITERAL
    | INTEGER_LITERAL
    | FLOAT_LITERAL
    | K_TRUE
    | K_FALSE
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
 */
K_MATCH : [Mm][Aa][Tt][Cc][Hh] ;
K_TRUE  : [Tt][Rr][Uu][Ee] ;
K_FALSE : [Ff][Aa][Ll][Ss][Ee] ;

/**
 * Two-character operators must be declared before the single DASH token
 * so that ANTLR4's maximal-munch rule chooses the longer match.
 */
ARROW    : '->' ;   // directed edge tail
LARROW   : '<-' ;   // reverse directed edge head

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
 * String literal: single-quoted, any characters except newline or unescaped quote.
 * Escape sequences are not supported in this minimal grammar.
 */
STRING_LITERAL : '\'' (~['\r\n])* '\'' ;

/**
 * Float literal: digits with a decimal point. Must be declared before INTEGER_LITERAL
 * so that ANTLR's maximal-munch rule prefers the longer token for inputs like "3.14".
 */
FLOAT_LITERAL : [0-9]+ '.' [0-9]* ;

/**
 * Integer literal: one or more decimal digits.
 */
INTEGER_LITERAL : [0-9]+ ;

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
