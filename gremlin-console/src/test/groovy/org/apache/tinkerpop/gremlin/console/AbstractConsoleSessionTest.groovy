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
import org.junit.After
import org.junit.Before

import java.nio.file.Files
import java.nio.file.Path

/**
 * Runs each test against consoles started in this JVM, with its own home directory and the preferences reset.
 */
abstract class AbstractConsoleSessionTest {

    static final String MODERN_GRAPH = "g = traversal().with(TinkerFactory.createModern())"
    static final String MODERN_GRAPH_RESULT = "==>graphtraversalsource[tinkermemorygraph[vertices:6 edges:6], standard]"

    // in-memory in tests (see InMemoryPreferencesFactory)
    protected static final java.util.prefs.Preferences STORE =
            java.util.prefs.Preferences.userRoot().node("/org/apache/tinkerpop/gremlin/console")

    private static boolean preferencesLoaded = false

    protected Path home
    private String oldHome
    private boolean oldAnsi
    private final List<ConsoleSession> sessions = []

    @Before
    void setUpSession() {
        // loading Console turns colors on, so load it before they are switched off below
        Class.forName(Console.name)
        // main() loads the preferences and installs their listeners before it creates the console
        synchronized (AbstractConsoleSessionTest) {
            if (!preferencesLoaded) {
                Preferences.expandoMagic()
                preferencesLoaded = true
            }
        }
        resetPreferences()
        home = Files.createTempDirectory("console-session-test")
        oldHome = System.getProperty("user.home")
        System.setProperty("user.home", home.toString())
        oldAnsi = Ansi.isEnabled()
        // as with -C
        Ansi.enabled = false
    }

    @After
    void tearDownSession() {
        // newest first, as each session restores the streams it replaced
        sessions.reverseEach { if (it != null) try { it.close() } catch (Throwable ignored) { } }
        System.setProperty("user.home", oldHome)
        Ansi.enabled = oldAnsi
        resetPreferences()
        home.toFile().deleteDir()
    }

    protected ConsoleSession start(final List<List<String>> scriptsAndArgs = null, final int columns = 120) {
        final ConsoleSession session = new ConsoleSession(home, scriptsAndArgs, columns)
        sessions << session
        return session
    }

    protected File writeFile(final String name, final String content) {
        final File file = home.resolve(name).toFile()
        file.parentFile.mkdirs()
        file.setText(content, "UTF-8")
        return file
    }

    /**
     * Sets up the modern graph as {@code g}, checking the variable on its own line since an assignment reports null.
     */
    protected static void loadModern(final ConsoleSession console) {
        console.send(MODERN_GRAPH)
        assert console.results("g") == [MODERN_GRAPH_RESULT]
    }

    /**
     * Waits for a preference set with {@code :set} to reach the console, which applies it on a background thread.
     */
    protected static void awaitPreference(final Closure<Boolean> applied) {
        ConsoleSession.waitUntil("the preference change", applied)
    }

    private static void resetPreferences() {
        STORE.clear()
        // the listeners reset the values asynchronously, so set the defaults directly as well
        Preferences.loadDefaultValues()
    }
}
