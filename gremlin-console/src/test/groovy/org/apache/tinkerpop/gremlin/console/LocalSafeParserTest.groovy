/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.console

import org.apache.groovy.groovysh.ParseCode
import org.apache.groovy.groovysh.ParseStatus
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull

/**
 * Unit tests for {@link LocalSafeParser}.
 */
class LocalSafeParserTest {
    
    private LocalSafeParser parser
    
    @Before
    void setUp() {
        parser = new LocalSafeParser()
    }
    
    @Test
    void shouldReturnCompleteForEmptyBuffer() {
        def status = parser.parse([])
        assertEquals(ParseCode.COMPLETE, status.code)
    }
    
    @Test
    void shouldReturnCompleteForNullBuffer() {
        def status = parser.parse(null)
        assertEquals(ParseCode.COMPLETE, status.code)
    }
    
    @Test
    void shouldReturnCompleteForValidScript() {
        def script = ["def x = 1", "println x"]
        def status = parser.parse(script)
        assertEquals(ParseCode.COMPLETE, status.code)
    }
    
    @Test
    void shouldReturnCompleteForValidSingleLineScript() {
        def script = ["def x = 1; println x"]
        def status = parser.parse(script)
        assertEquals(ParseCode.COMPLETE, status.code)
    }
    
    @Test
    void shouldReturnIncompleteForUnclosedCurlyBracket() {
        def script = ["if (true) {", "  println 'hello'"]
        def status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }
    
    @Test
    void shouldReturnIncompleteForUnclosedParenthesis() {
        def script = ["println(1 + 2"]
        def status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }
    
    @Test
    void shouldReturnIncompleteForUnclosedSquareBracket() {
        def script = ["def list = [1, 2, 3"]
        def status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }
    
    @Test
    void shouldReturnIncompleteForUnclosedSingleQuoteString() {
        def script = ["def s = 'unclosed string"]
        def status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }
    
    @Test
    void shouldReturnIncompleteForUnclosedDoubleQuoteString() {
        def script = ["def s = \"unclosed string"]
        def status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }
    
    @Test
    void shouldReturnIncompleteForUnclosedTripleQuoteString() {
        def script = ["def s = '''unclosed triple quote string"]
        ParseStatus status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }
    
    @Test
    void shouldReturnIncompleteForUnexpectedEOF() {
        def script = ["def method() {"]
        def status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }
    
    @Test
    void shouldReturnIncompleteForUnexpectedInput() {
        def script = ["def x = 1 +"]
        def status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }

    @Test
    void shouldReturnIncompleteForInvalidSyntax() {
        // Variable name cannot start with a number
        def script = ["def 1x = 1"]
        def status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }
    
    @Test
    void shouldReturnCompleteForSyntaxError() {
        def script = ["def x = 1;", "x.nonExistentMethod()"]
        def status = parser.parse(script)
        assertEquals(ParseCode.COMPLETE, status.code)
    }
    
    @Test
    void shouldReturnErrorForUnexpectedClosingBracket() {
        def script = ["def x = 1}", "println x"]
        def status = parser.parse(script)
        assertEquals(ParseCode.ERROR, status.code)
        assertNotNull(status.cause)
    }
    
    @Test
    void shouldReturnIncompleteForMultilineIncompleteScript() {
        def script = [
            "def method() {",
            "  if (true) {",
            "    println 'hello'",
            "  }"
        ]

        def status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }
    
    @Test
    void shouldReturnCompleteForMultilineCompleteScript() {
        def script = [
            "def method() {",
            "  if (true) {",
            "    println 'hello'",
            "  }",
            "}"
        ]

        def status = parser.parse(script)
        assertEquals(ParseCode.COMPLETE, status.code)
    }
    
    @Test
    void shouldReturnIncompleteForIncompleteGremlinTraversal() {
        def script = ["g.V().has("]
        def status = parser.parse(script)
        assertEquals(ParseCode.INCOMPLETE, status.code)
    }
    
    @Test
    void shouldReturnCompleteForCompleteGremlinTraversal() {
        def script = ["g.V().count()"]
        def status = parser.parse(script)
        assertEquals(ParseCode.COMPLETE, status.code)
    }
}