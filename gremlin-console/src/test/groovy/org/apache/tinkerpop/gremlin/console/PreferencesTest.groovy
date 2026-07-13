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

import org.codehaus.groovy.tools.shell.util.Preferences as ShellPreferences
import org.codehaus.groovy.tools.shell.IO
import org.fusesource.jansi.Ansi
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

import static org.junit.Assert.assertEquals

/**
 * Exercises the {@link Preferences} change listeners installed by {@code expandoMagic()}. Changing a preference
 * through {@code org.codehaus.groovy.tools.shell.util.Preferences.put(key, value)} writes to the backing
 * {@code java.util.prefs} store which asynchronously notifies the listeners, so the assertions poll for the
 * expected static field update.
 */
class PreferencesTest {

    private static boolean originalAnsiEnabled

    // A known-valid jansi color name and a name guaranteed to make Colorizer.render throw.
    private static final String VALID_COLOR = "red"
    private static final String INVALID_COLOR = "notavalidcolorname"

    // Maps each color preference key to a closure that reads the corresponding static field.
    private static final Map<String, Closure<String>> COLOR_FIELDS = [
            (Preferences.PREF_GREMLIN_COLOR)      : { Preferences.gremlinColor },
            (Preferences.PREF_VERTEX_COLOR)       : { Preferences.vertexColor },
            (Preferences.PREF_EDGE_COLOR)         : { Preferences.edgeColor },
            (Preferences.PREF_ERROR_COLOR)        : { Preferences.errorColor },
            (Preferences.PREF_INFO_COLOR)         : { Preferences.infoColor },
            (Preferences.PREF_STRING_COLOR)       : { Preferences.stringColor },
            (Preferences.PREF_NUMBER_COLOR)       : { Preferences.numberColor },
            (Preferences.PREF_T_COLOR)            : { Preferences.tColor },
            (Preferences.PREF_INPUT_PROMPT_COLOR) : { Preferences.inputPromptColor },
            (Preferences.PREF_RESULT_PROMPT_COLOR): { Preferences.resultPromptColor }
    ]

    // Maps each color preference key to its default value used for the invalid-color fallback.
    private static final Map<String, String> COLOR_DEFAULTS = [
            (Preferences.PREF_GREMLIN_COLOR)      : Preferences.PREF_GREMLIN_COLOR_DEFAULT,
            (Preferences.PREF_VERTEX_COLOR)       : Preferences.PREF_VERTEX_COLOR_DEFAULT,
            (Preferences.PREF_EDGE_COLOR)         : Preferences.PREF_EDGE_COLOR_DEFAULT,
            (Preferences.PREF_ERROR_COLOR)        : Preferences.PREF_ERROR_COLOR_DEFAULT,
            (Preferences.PREF_INFO_COLOR)         : Preferences.PREF_INFO_COLOR_DEFAULT,
            (Preferences.PREF_STRING_COLOR)       : Preferences.PREF_STRING_COLOR_DEFAULT,
            (Preferences.PREF_NUMBER_COLOR)       : Preferences.PREF_NUMBER_COLOR_DEFAULT,
            (Preferences.PREF_T_COLOR)            : Preferences.PREF_T_COLOR_DEFAULT,
            (Preferences.PREF_INPUT_PROMPT_COLOR) : Preferences.PREF_INPUT_PROMPT_COLOR_DEFAULT,
            (Preferences.PREF_RESULT_PROMPT_COLOR): Preferences.PREF_RESULT_PROMPT_COLOR_DEFAULT
    ]

    @BeforeClass
    static void setUpClass() {
        // Enable ansi so that Colorizer.render actually validates color names (and can throw on invalid ones),
        // which drives the getValidColor catch/fallback branch. Preserve the prior state for restoration.
        originalAnsiEnabled = Ansi.isEnabled()
        Ansi.setEnabled(true)
        Preferences.colors = true

        // Installs the change listeners AND loads default values.
        Preferences.expandoMagic()
    }

    @AfterClass
    static void tearDownClass() {
        // These tests write to the real per-user java.util.prefs store. Clear it so developer prefs aren't polluted.
        ShellPreferences.clear()
        Ansi.setEnabled(originalAnsiEnabled)
    }

    @Test
    void shouldUpdateColorFieldsWhenValidColorSet() {
        COLOR_FIELDS.each { key, field ->
            putAndWait(key, VALID_COLOR) { field() == VALID_COLOR }
            assertEquals("field for ${key} should update to the valid color".toString(), VALID_COLOR, field())
        }
    }

    @Test
    void shouldFallBackToDefaultWhenInvalidColorSet() {
        // getValidColor only falls back when Colorizer.render throws, which requires ansi + colors enabled.
        Ansi.setEnabled(true)
        Preferences.colors = true

        COLOR_DEFAULTS.each { key, defaultValue ->
            def field = COLOR_FIELDS[key]
            putAndWait(key, INVALID_COLOR) { field() == defaultValue }
            assertEquals("field for ${key} should fall back to its default".toString(), defaultValue, field())
        }
    }

    @Test
    void shouldUpdateMaxIterationWithValidInteger() {
        putAndWait(Preferences.PREFERENCE_ITERATION_MAX, "50") { Preferences.maxIteration == 50 }
        assertEquals(50, Preferences.maxIteration)
    }

    @Test
    void shouldFallBackToDefaultMaxIterationWithInvalidInteger() {
        putAndWait(Preferences.PREFERENCE_ITERATION_MAX, "not-a-number") { Preferences.maxIteration == 100 }
        assertEquals(100, Preferences.maxIteration)
    }

    @Test
    void shouldUpdateEmptyResult() {
        putAndWait(Preferences.PREF_RESULT_IND_NULL, "nothing") { Preferences.emptyResult == "nothing" }
        assertEquals("nothing", Preferences.emptyResult)
    }

    @Test
    void shouldUpdateInputPrompt() {
        putAndWait(Preferences.PREF_INPUT_PROMPT, "in>") { Preferences.inputPrompt == "in>" }
        assertEquals("in>", Preferences.inputPrompt)
    }

    @Test
    void shouldUpdateResultPrompt() {
        putAndWait(Preferences.PREF_RESULT_PROMPT, "res>") { Preferences.resultPrompt == "res>" }
        assertEquals("res>", Preferences.resultPrompt)
    }

    @Test
    void shouldUpdateColorsBoolean() {
        putAndWait(Preferences.PREF_COLORS, "false") { !Preferences.colors }
        assertEquals(false, Preferences.colors)

        putAndWait(Preferences.PREF_COLORS, "true") { Preferences.colors }
        assertEquals(true, Preferences.colors)
    }

    @Test
    void shouldUpdateWarningsBoolean() {
        putAndWait(Preferences.PREF_WARNINGS, "false") { !Preferences.warnings }
        assertEquals(false, Preferences.warnings)

        putAndWait(Preferences.PREF_WARNINGS, "true") { Preferences.warnings }
        assertEquals(true, Preferences.warnings)
    }

    @Test
    void shouldUpdateVerbosityWhenValidValueSet() {
        // exercise the verbosity change listener installed by expandoMagic() with a valid verbosity name
        putAndWait(ShellPreferences.VERBOSITY_KEY, IO.Verbosity.DEBUG.name) {
            ShellPreferences.verbosity == IO.Verbosity.DEBUG
        }
        assertEquals(IO.Verbosity.DEBUG, ShellPreferences.verbosity)
    }

    @Test
    void shouldRestoreVerbosityWhenInvalidValueSet() {
        // establish a known-good verbosity first
        putAndWait(ShellPreferences.VERBOSITY_KEY, IO.Verbosity.INFO.name) {
            ShellPreferences.verbosity == IO.Verbosity.INFO
        }
        assertEquals(IO.Verbosity.INFO, ShellPreferences.verbosity)

        // an invalid value makes IO.Verbosity.forName throw; the listener's catch branch re-writes the current
        // valid verbosity name back to the store, so the effective verbosity is left unchanged
        ShellPreferences.put(ShellPreferences.VERBOSITY_KEY, "notaverbosity")
        final long deadline = System.currentTimeMillis() + 3000
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(10)
        }
        assertEquals(IO.Verbosity.INFO, ShellPreferences.verbosity)
    }

    /**
     * Writes a preference and waits (up to a timeout) for the asynchronously delivered change event to update the
     * relevant static field, guarding against the non-deterministic timing of the java.util.prefs event dispatch.
     */
    private static void putAndWait(final String key, final String value, final Closure<Boolean> condition) {
        ShellPreferences.put(key, value)
        final long deadline = System.currentTimeMillis() + 5000
        while (!condition.call() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10)
        }
    }
}
