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

import org.jline.jansi.Ansi
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

import static org.junit.Assert.assertEquals

/**
 * Exercises the {@link Preferences} change listeners installed by {@code expandoMagic()}, focusing on the
 * validation/fallback branches (bad color, bad integer). Preference writes reach the listeners asynchronously via
 * the backing {@code java.util.prefs} store, so assertions poll for the update.
 */
class PreferencesTest {

    // in-memory in tests (see InMemoryPreferencesFactory)
    private static final java.util.prefs.Preferences STORE =
            java.util.prefs.Preferences.userRoot().node("/org/apache/tinkerpop/gremlin/console")

    private static boolean originalAnsiEnabled

    @BeforeClass
    static void setUpClass() {
        // Ansi must be on for Colorizer.render to validate colors and throw, driving the getValidColor fallback.
        originalAnsiEnabled = Ansi.isEnabled()
        Ansi.setEnabled(true)
        Preferences.colors = true

        // Installs the change listeners AND loads default values.
        Preferences.expandoMagic()
    }

    @AfterClass
    static void tearDownClass() {
        Ansi.setEnabled(originalAnsiEnabled)
        STORE.clear()
    }

    @Test
    void shouldUpdateColorOnValidValueAndFallBackOnInvalid() {
        // getValidColor only falls back when Colorizer.render throws, which requires ansi + colors enabled
        // (another test may have turned colors off, and test order is not guaranteed).
        Ansi.setEnabled(true)
        Preferences.colors = true

        // valid color drives the getValidColor success path
        putAndWait(Preferences.PREF_GREMLIN_COLOR, "red") { Preferences.gremlinColor == "red" }
        assertEquals("red", Preferences.gremlinColor)

        // an unrenderable color makes Colorizer.render throw, driving the getValidColor catch/fallback branch
        putAndWait(Preferences.PREF_GREMLIN_COLOR, "notavalidcolorname") {
            Preferences.gremlinColor == Preferences.PREF_GREMLIN_COLOR_DEFAULT
        }
        assertEquals(Preferences.PREF_GREMLIN_COLOR_DEFAULT, Preferences.gremlinColor)
    }

    @Test
    void shouldUpdateMaxIterationOnValidValueAndFallBackOnInvalid() {
        putAndWait(Preferences.PREFERENCE_ITERATION_MAX, "50") { Preferences.maxIteration == 50 }
        assertEquals(50, Preferences.maxIteration)

        // a non-numeric value drives the NumberFormatException fallback to the default
        putAndWait(Preferences.PREFERENCE_ITERATION_MAX, "not-a-number") { Preferences.maxIteration == 100 }
        assertEquals(100, Preferences.maxIteration)
    }

    @Test
    void shouldUpdateStringPreferences() {
        putAndWait(Preferences.PREF_RESULT_IND_NULL, "nothing") { Preferences.emptyResult == "nothing" }
        assertEquals("nothing", Preferences.emptyResult)

        putAndWait(Preferences.PREF_INPUT_PROMPT, "in>") { Preferences.inputPrompt == "in>" }
        assertEquals("in>", Preferences.inputPrompt)

        putAndWait(Preferences.PREF_RESULT_PROMPT, "res>") { Preferences.resultPrompt == "res>" }
        assertEquals("res>", Preferences.resultPrompt)
    }

    @Test
    void shouldUpdateBooleanPreferences() {
        putAndWait(Preferences.PREF_COLORS, "false") { !Preferences.colors }
        assertEquals(false, Preferences.colors)

        putAndWait(Preferences.PREF_WARNINGS, "false") { !Preferences.warnings }
        assertEquals(false, Preferences.warnings)
    }

    /**
     * Writes a preference and polls until the listener updates the target field, or the timeout expires.
     */
    private static void putAndWait(final String key, final String value, final Closure<Boolean> condition) {
        STORE.put(key, value)
        final long deadline = System.currentTimeMillis() + 5000
        while (!condition.call() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10)
        }
    }
}
