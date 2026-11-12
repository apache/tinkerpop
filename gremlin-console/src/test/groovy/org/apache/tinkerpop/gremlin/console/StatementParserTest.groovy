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
package org.apache.tinkerpop.gremlin.console

import org.jline.reader.EOFError
import org.jline.reader.Parser.ParseContext
import org.jline.reader.impl.DefaultParser
import org.junit.Test

import java.util.function.Predicate

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

class StatementParserTest {
    // configured like the parser groovysh gives the line reader
    private final DefaultParser lineParser = new DefaultParser(eofOnUnclosedQuote: true, eofOnEscapedNewLine: true)
            .eofOnUnclosedBracket(DefaultParser.Bracket.CURLY, DefaultParser.Bracket.ROUND, DefaultParser.Bracket.SQUARE)

    private final StatementParser parser = new StatementParser(lineParser, Console.createEngine().compilerConfiguration,
            { it == "/set" || it == "/plugin" } as Predicate<String>)

    @Test
    void shouldWaitWhenAStatementEndsTooEarly() {
        for (String unfinished : ["g.V().", "g.V().out().", "'abc'.", "1 +", "x = 1 +", "x ==", "x &&", "a ? b :",
                                  "if (x)", "g.V().\n  out().", "x <<", "x = g.V().",
                                  // Groovy reports these at the repeat() call rather than at the trailing "."
                                  "repeat(bothE()).", "g.V().\n  repeat(out()).",
                                  "g.V(ids).as('a').\n  repeat(bothE().otherV().simplePath()).times(5).as('b').",
                                  "g.V().where(out().count().is(gt(1)))."]) {
            assertTrue(unfinished, parser.continuesOnNextLine(unfinished))
            assertWaits(unfinished)
        }
    }

    @Test
    void shouldAcceptCompleteStatements() {
        for (String complete : ["g.V()", "g.V().\n  out()", "g.V()\n  .out()", "x = 1", "x = 1;", "1 + 1", "def f() { 1 }"]) {
            assertFalse(complete, parser.continuesOnNextLine(complete))
            assertAccepted(complete)
        }
    }

    @Test
    void shouldReportMistakesBeforeTheEndAtOnce() {
        // the syntax error is not at the end, so more input would not help
        for (String mistake : ["x = = 1", "1 + + 2", ":nope", ":", "g.V()..out()", "x = 5 5", "g.V().1"]) {
            assertFalse(mistake, parser.continuesOnNextLine(mistake))
            assertAccepted(mistake)
        }
    }

    @Test
    void shouldKeepWaitingForOpenBrackets() {
        for (String open : ["[1,2,", "foo(", "class A {"]) {
            assertWaits(open)
        }
    }

    @Test
    void shouldAcceptAStrayClosingBracket() {
        // the line reader's parser waits for the missing opening bracket, which no further input can supply
        assertAccepted("x = )")
        assertAccepted("[1, 2]]")
    }

    @Test
    void shouldAcceptCommandsAsTheyAre() {
        assertAccepted(':set result.prompt "oops')
        assertAccepted(":plugin list")
        // not a command, so still a Groovy string that is not closed yet
        assertWaits('x = "oops')
    }

    @Test
    void shouldOnlyCheckLinesThatAreAccepted() {
        assertEquals(["g.V()."], parser.parse("g.V().", 6, ParseContext.COMPLETE).words())
    }

    @Test
    void shouldSplitScriptsLikeTypedInput() {
        final StatementParser script = StatementParser.forScripts(Console.createEngine().compilerConfiguration)
        // open brackets, strings and comments, including a license header at the top of a script
        for (String unfinished : ["[1,2,", "foo(", "foo(1, [2, 3]", "add3 = { a ->", "if (true) {", "class Person {",
                                  "s = \'\'\'", "s = \"\"\"", "/*", "/**\n * license header", "x = 1 /* note",
                                  "x = 1 +", "g.V(1).", "x ==", "if (x)", "g.V().\n  repeat(out()).", 'x = [")",']) {
            assertTrue(unfinished, script.isUnfinished(unfinished))
        }
        // more lines cannot fix these, so they run and fail at once rather than swallowing the rest of the script
        for (String done : ["x = 1", "g.V()", "x = = 1", "1 + + 2", "x = )", "x = ]", "}", "[1, 2))", "foo())",
                            "// comment"]) {
            assertFalse(done, script.isUnfinished(done))
        }
    }

    @Test
    void shouldIgnoreBracketsInStringsWhenWaiting() {
        assertWaits('x = [")",')
        assertWaits('m = ["}": 1,')
        assertWaits('foo("))"')
        assertAccepted('x = ")")')
        assertAccepted("x = 1 // )")
    }

    private void assertWaits(final String input) {
        try {
            parser.parse(input, input.length(), ParseContext.ACCEPT_LINE)
            fail("expected to wait for more input after: " + input)
        } catch (EOFError ignored) {
            // waits
        }
    }

    private void assertAccepted(final String input) {
        try {
            parser.parse(input, input.length(), ParseContext.ACCEPT_LINE)
        } catch (EOFError e) {
            fail("expected to accept: " + input + " but got " + e.message)
        }
    }
}
