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
package org.apache.tinkerpop.gremlin.console.commands

import org.apache.tinkerpop.gremlin.console.Preferences
import org.jline.reader.Completer
import org.jline.shell.CommandSession
import org.junit.After
import org.junit.Before
import org.junit.Test

import static org.apache.tinkerpop.gremlin.console.commands.CommandCompletion.complete
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertSame
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

/**
 * Unit tests for {@link GremlinSetCommand} that exercise the Gremlin-specific completer and the preference
 * setting/listing behaviour without a live Gremlin server, Grape, or network access.
 */
class GremlinSetCommandTest {

    // in-memory in tests (see InMemoryPreferencesFactory)
    private static final java.util.prefs.Preferences STORE =
            java.util.prefs.Preferences.userRoot().node("/org/apache/tinkerpop/gremlin/console")

    private ByteArrayOutputStream out
    private GremlinSetCommand command

    @Before
    void setUp() {
        out = new ByteArrayOutputStream()
        command = new GremlinSetCommand()
        command.out = new PrintStream(out, true)
    }

    @After
    void tearDown() {
        STORE.clear()
    }

    @Test
    void shouldCreateCompletersForTheCommandWordAndTheName() {
        final List<Completer> completers = command.completers()
        assertNotNull(completers)
        assertEquals(2, completers.size())
        assertSame(GremlinCommand.COMMAND_WORD, completers[0])
        assertNotNull(completers[1])
    }

    @Test
    void shouldIncludeGremlinSpecificPreferencesInCompleter() {
        final List<String> candidateStrings = complete(command, "/set ")

        // Gremlin Console specific preferences that the base SetCommand does not offer.
        assertTrue("expected max-iteration in completer candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREFERENCE_ITERATION_MAX))
        assertTrue("expected result.indicator.null in completer candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREF_RESULT_IND_NULL))
        assertTrue("expected vertex.color in completer candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREF_VERTEX_COLOR))
        assertTrue("expected input.prompt in completer candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREF_INPUT_PROMPT))
        assertTrue("expected warnings in completer candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREF_WARNINGS))
    }

    @Test
    void shouldFilterCompleterCandidatesByPrefix() {
        final List<String> candidateStrings = complete(command, "/set max")
        assertTrue("expected only 'max' prefixed candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREFERENCE_ITERATION_MAX))
        assertFalse("did not expect unrelated keys: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREF_VERTEX_COLOR))
    }

    @Test
    void shouldSetAndRetrieveKnownPreference() {
        command.execute([Preferences.PREFERENCE_ITERATION_MAX, "42"])
        assertEquals("42", STORE.get(Preferences.PREFERENCE_ITERATION_MAX, null))
    }

    @Test
    void shouldSetBooleanPreferenceToTrueWhenValueOmitted() {
        // With a single argument the value defaults to "true".
        command.execute([Preferences.PREF_WARNINGS])
        assertEquals("true", STORE.get(Preferences.PREF_WARNINGS, null))
    }

    @Test
    void shouldListPreferencesWhenNoArguments() {
        command.execute([Preferences.PREF_RESULT_PROMPT, "==>"])
        out.reset()

        command.execute([])

        final String output = out.toString()
        assertTrue("expected a preferences listing but got: ${output}".toString(),
                output.contains("Preferences:") && output.contains(Preferences.PREF_RESULT_PROMPT))
    }

    // the checks groovysh 4's SetCommand used to provide

    @Test
    void shouldReportWhenNoPreferencesAreSet() {
        command.execute([])
        assertTrue(out.toString(), out.toString().contains("No preferences are set"))
    }

    @Test(expected = UsageException)
    void shouldRejectTooManyArguments() {
        command.execute(["a", "b", "c"])
    }

    @Test
    void shouldCompleteStoredPreferenceNames() {
        STORE.put("custom.setting", "x")
        assertTrue(complete(command, "/set ").contains("custom.setting"))
    }

    // added with Groovy 6

    @Test
    void shouldRejectAnArgumentWithAnUnclosedQuote() {
        try {
            command.execute(session(), ["result.prompt", '"oops'] as String[])
            fail("expected an unclosed quote to be rejected")
        } catch (IllegalArgumentException e) {
            assertEquals('Missing closing "', e.message)
        }
    }

    @Test
    void shouldAcceptHelpFlagAsValue() {
        command.execute(session(), [Preferences.PREF_RESULT_PROMPT, "-?"] as String[])
        assertEquals("-?", STORE.get(Preferences.PREF_RESULT_PROMPT, null))
    }

    private CommandSession session() {
        return new CommandSession(null, new ByteArrayInputStream(new byte[0]), new PrintStream(out, true),
                new PrintStream(new ByteArrayOutputStream(), true))
    }
}
