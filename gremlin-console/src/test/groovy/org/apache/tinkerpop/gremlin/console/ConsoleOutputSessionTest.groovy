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

import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue

/**
 * How results are printed: iteration, the result limit, empty and null results, prompts and explain() width.
 */
class ConsoleOutputSessionTest extends AbstractConsoleSessionTest {

    @Test
    void shouldPrintEveryResultOfATraversalInOrder() {
        final ConsoleSession console = start()
        loadModern(console)
        assertEquals((1..6).collect { "==>v[$it]".toString() }, console.results("g.V()"))
        assertEquals(["==>marko", "==>vadas", "==>lop", "==>josh", "==>ripple", "==>peter"],
                console.results("g.V().values('name')"))
    }

    @Test
    void shouldPrintOneHundredResultsThenAnEllipsis() {
        final ConsoleSession console = start()
        final List<String> lines = console.send("(1..101).toList()")
        assertEquals((1..100).collect { "==>$it".toString() }, lines.findAll { it.startsWith("==>") })
        assertEquals("...", lines[-1])
    }

    @Test
    void shouldNotPrintAnEllipsisForExactlyOneHundredResults() {
        final ConsoleSession console = start()
        final List<String> lines = console.send("(1..100).toList()")
        assertEquals(100, lines.findAll { it.startsWith("==>") }.size())
        assertFalse(lines.contains("..."))
    }

    @Test
    void shouldPrintNothingForEmptyResults() {
        final ConsoleSession console = start()
        loadModern(console)
        assertEquals([], console.send("[]"))
        assertEquals([], console.send("[:]"))
        assertEquals([], console.send("g.V().has('name', 'nobody')"))
    }

    @Test
    void shouldAppendToAListThatWasPrintedEmpty() {
        // Console.groovy has special handling so that printing an empty list does not keep its iterator around, which
        // would otherwise make the next line fail with a ConcurrentModificationException
        final ConsoleSession console = start()
        assertEquals(["==>null"], console.send("x = []"))
        assertEquals([], console.send("x"))
        assertEquals(["==>test"], console.results("x << 'test'"))
        assertEquals(["==>test", "==>more"], console.results("x << 'more'"))
    }

    @Test
    void shouldPrintEachEntryOfATopLevelMap() {
        final ConsoleSession console = start()
        assertEquals(["==>a=1", "==>b=2"], console.results("[a: 1, b: 2]"))
        // a map nested in a list is printed whole
        assertEquals(["==>[a:1]"], console.results("[[a: 1]]"))
    }

    @Test
    void shouldPrintTheOutputOfPrintlnThenANullResult() {
        final ConsoleSession console = start()
        assertEquals(["hi", "==>null"], console.send("println 'hi'"))
        assertEquals(["==>null"], console.results("null"))
    }

    @Test
    void shouldPrintNullForAnAssignmentAndSetTheVariable() {
        final ConsoleSession console = start()
        assertEquals(["==>null"], console.results("x = 5"))
        assertEquals(["==>null"], console.results("s = 'text'"))
        assertEquals(["==>5"], console.results("x"))
        assertEquals(["==>text"], console.results("s"))
        // an assigned traversal is not iterated by the assignment
        loadModern(console)
        assertEquals(["==>null"], console.results("t = g.V().values('name')"))
        assertEquals(["==>6"], console.results("t.count()"))
    }

    @Test
    void shouldRoundTripUnicode() {
        final ConsoleSession console = start()
        assertEquals(["==>h\u00e9llo \u65e5\u672c"], console.results("'h\u00e9llo \u65e5\u672c'"))
        assertEquals(["==>2"], console.results("'\u65e5\u672c'.length()"))
    }

    /**
     * Prints a long explain() in a terminal of the given width and returns the lengths of the printed lines.
     */
    private List<Integer> explainLineLengths(final int columns) {
        final ConsoleSession console = start(null, columns)
        loadModern(console)
        // build the long traversal inside a list so that it is printed rather than iterated
        console.send("x = [g.V().out().in().out().in().out().in().out().in().out().in()]")
        final List<String> lines = console.send("x[0].has('name', 'marko').values('name').explain()")
        assertEquals(lines.toString(), "==>Traversal Explanation", lines[0])
        return lines.collect { it.replaceAll(/\s+$/, "").length() }
    }

    @Test
    void shouldFitExplainToANarrowTerminal() {
        assertTrue(explainLineLengths(100).max() <= 100)
    }

    @Test
    void shouldUseTheFullWidthOfAWideTerminalForExplain() {
        // the same explanation is not wrapped at 100 columns when the terminal is wider
        assertTrue(explainLineLengths(400).max() > 100)
    }

    @Test
    void shouldUseAQuotedResultPromptWithATrailingSpace() {
        final ConsoleSession console = start()
        // the null result of :set itself may already use the new prompt, so its output is not checked
        console.send(':set result.prompt "--> "')
        awaitPreference { Preferences.resultPrompt == "--> " }
        assertEquals(["--> 1"], console.send("1"))
    }

    @Test
    void shouldRemoveTheLimitWithMaxIterationMinusOne() {
        final ConsoleSession console = start()
        assertEquals(["==>null"], console.send(":set max-iteration -1"))
        awaitPreference { Preferences.maxIteration == -1 }
        final List<String> lines = console.send("(1..500).toList()")
        assertEquals(500, lines.findAll { it.startsWith("==>") }.size())
        assertFalse(lines.contains("..."))
    }

    @Test
    void shouldFallBackToOneHundredForAnInvalidMaxIteration() {
        final ConsoleSession console = start()
        console.send(":set max-iteration abc")
        // preference changes are applied on a background thread, so the warning may come after the next prompt
        console.out.waitFor("Unable to convert 'abc' to integer. Using default 100", 0)
        final List<String> lines = console.send("(1..101).toList()")
        assertEquals(100, lines.findAll { it.startsWith("==>") }.size())
    }

    @Test
    void shouldPrintAnIteratorOnlyOnce() {
        final ConsoleSession console = start()
        assertEquals(["==>null"], console.results("it = [1, 2].iterator()"))
        assertEquals(["==>1", "==>2"], console.results("it"))
        // printing it used it up
        assertEquals([], console.results("it"))
        assertEquals(["==>false"], console.results("it.hasNext()"))
    }

    @Test
    void shouldChangeOrHideTheNullResultIndicator() {
        final ConsoleSession console = start()
        // the null result of :set itself may already use the new indicator, so its output is not checked
        console.send(":set result.indicator.null nothing")
        awaitPreference { Preferences.emptyResult == "nothing" }
        assertEquals(["==>nothing"], console.send("null"))
        console.send(':set result.indicator.null ""')
        awaitPreference { Preferences.emptyResult == "" }
        // an empty indicator prints no line at all
        assertEquals([], console.send("null"))
    }

    @Test
    void shouldPrintTokensAndEnums() {
        final ConsoleSession console = start()
        assertEquals(["==>id"], console.results("T.id"))
        assertEquals(["==>label"], console.results("T.label"))
    }

    @Test
    void shouldPrintTheMetricsOfProfile() {
        final ConsoleSession console = start()
        loadModern(console)
        final String text = console.send("g.V().out().profile()").join("\n")
        ["Step", "Count", "Traversers", "Time (ms)", "TinkerGraphStep", ">TOTAL"].each {
            assertTrue(text, text.contains(it))
        }
    }

    @Test
    void shouldTakeTheResultPromptFromASystemProperty() {
        final String old = System.getProperty("gremlin.prompt")
        System.setProperty("gremlin.prompt", "prop>")
        try {
            assertEquals(["prop>1"], start().send("1"))
        } finally {
            if (old == null) System.clearProperty("gremlin.prompt") else System.setProperty("gremlin.prompt", old)
        }
    }

    @Test
    void shouldChangeTheInputPrompt() {
        final ConsoleSession console = start()
        console.type(":set input.prompt g>")
        awaitPreference { Preferences.inputPrompt == "g>" }
        // the change is applied on a background thread, so the prompt right after :set can be the old or the new one
        console.send("'synced'", "g> ")
        assertEquals(["==>2"], console.results("1+1", "g> "))
        // the continuation prompt is padded to the width of the new prompt
        console.send("[1,", "1> ")
        assertEquals(["==>1", "==>2"], console.results("2]", "g> "))
    }

    @Test
    void shouldNotCutALongStringResult() {
        final ConsoleSession console = start(null, 400)
        final List<String> lines = console.send("'x' * 5000")
        assertEquals("==>" + "x" * 5000, lines.collect { it.trim() }.join(""))
    }

    @Test
    void shouldReportAnInvalidColorAndFallBackToTheDefault() {
        final ConsoleSession console = start()
        console.send(":set vertex.color notacolor")
        // printed by the background preference listener, before or after the next prompt
        console.out.waitFor("Invalid color option for vertex.color: notacolor", 0)
        loadModern(console)
        assertEquals(["==>v[1]"], console.results("g.V(1)"))
    }
}
