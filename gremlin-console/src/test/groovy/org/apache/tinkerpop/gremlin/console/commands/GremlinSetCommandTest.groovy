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

import jline.console.completer.Completer
import org.apache.tinkerpop.gremlin.console.GremlinGroovysh
import org.apache.tinkerpop.gremlin.console.Mediator
import org.apache.tinkerpop.gremlin.console.Preferences
import org.codehaus.groovy.tools.shell.IO
import org.codehaus.groovy.tools.shell.util.Preferences as ShellPreferences
import org.junit.After
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertTrue

/**
 * Unit tests for {@link GremlinSetCommand} that exercise the Gremlin-specific completer and the preference
 * setting/listing behaviour without a live Gremlin server, Grape, or network access.
 */
class GremlinSetCommandTest {

    private ByteArrayOutputStream out
    private ByteArrayOutputStream err
    private GremlinGroovysh shell
    private GremlinSetCommand command

    @Before
    void setUp() {
        out = new ByteArrayOutputStream()
        err = new ByteArrayOutputStream()
        final IO testio = new IO(new ByteArrayInputStream(), out, err)
        shell = new GremlinGroovysh(new Mediator(null), testio)
        command = new GremlinSetCommand(shell)
    }

    @After
    void tearDown() {
        // These tests write to the real per-user java.util.prefs store, so clear it to avoid polluting dev prefs.
        ShellPreferences.clear()
    }

    @Test
    void shouldCreateCompletersWithNullSecondEntry() {
        final List<Completer> completers = command.createCompleters()
        assertNotNull(completers)
        assertEquals(2, completers.size())
        assertNotNull(completers[0])
        assertNull(completers[1])
    }

    @Test
    void shouldIncludeGremlinSpecificPreferencesInCompleter() {
        final Completer completer = command.createCompleters()[0]
        final List<CharSequence> candidates = []
        completer.complete("", 0, candidates)

        final List<String> candidateStrings = candidates.collect { it.toString().trim() }

        // Gremlin Console specific preferences that the base SetCommand does not offer.
        assertTrue("expected max-iteration in completer candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREFERENCE_ITERATION_MAX))
        assertTrue("expected result.indicator.null in completer candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREF_RESULT_IND_NULL))
        assertTrue("expected vertex.color in completer candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREF_VERTEX_COLOR))
        assertTrue("expected input.prompt in completer candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREF_INPUT_PROMPT))
    }

    @Test
    void shouldFilterCompleterCandidatesByPrefix() {
        final Completer completer = command.createCompleters()[0]
        final List<CharSequence> candidates = []
        completer.complete("max", 3, candidates)

        final List<String> candidateStrings = candidates.collect { it.toString().trim() }
        assertTrue("expected only 'max' prefixed candidates: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREFERENCE_ITERATION_MAX))
        assertFalse("did not expect unrelated keys: ${candidateStrings}".toString(),
                candidateStrings.contains(Preferences.PREF_VERTEX_COLOR))
    }

    @Test
    void shouldSetAndRetrieveKnownPreference() {
        command.execute([Preferences.PREFERENCE_ITERATION_MAX, "42"])
        assertEquals("42", ShellPreferences.get(Preferences.PREFERENCE_ITERATION_MAX, null))
    }

    @Test
    void shouldSetBooleanPreferenceToTrueWhenValueOmitted() {
        // With a single argument the value defaults to "true".
        command.execute([Preferences.PREF_WARNINGS])
        assertEquals("true", ShellPreferences.get(Preferences.PREF_WARNINGS, null))
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
}
