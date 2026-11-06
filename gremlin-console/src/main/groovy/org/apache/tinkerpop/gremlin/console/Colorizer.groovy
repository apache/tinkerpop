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

package org.apache.tinkerpop.gremlin.console;

import org.jline.jansi.Ansi
import org.jline.jansi.AnsiColors
import org.jline.jansi.AnsiMode
import org.jline.jansi.AnsiPrintStream
import org.jline.jansi.AnsiType
import org.jline.jansi.io.AnsiOutputStream
import org.jline.terminal.spi.SystemStream
import org.jline.terminal.spi.TerminalProvider

import java.nio.charset.Charset

public class Colorizer {

    public static void installAnsi() {
        // must be called before IO(), because IO stores refs to System.out and System.err.
        // JLine's AnsiConsole.systemInstall(), which replaces the jansi one used with Groovy 4, writes System.err to
        // standard output as well, so errors would mix into redirected results. each stream is wrapped here instead.
        System.setOut(ansiStream(System.out, SystemStream.Output))
        System.setErr(ansiStream(System.err, SystemStream.Error))
        Ansi.enabled = true
    }

    public static String render(String color, Object text) {
        if (Ansi.isEnabled() && Preferences.colors) {
            Ansi.ansi().render(String.format("@|%s %s|@", color, text)).toString()
        } else {
            return text
        }
    }

    /**
     * Keeps color codes when the stream is a terminal and removes them when it is redirected.
     */
    private static PrintStream ansiStream(final PrintStream stream, final SystemStream which) {
        if (isTerminal(which)) return stream
        final AnsiOutputStream strip = new AnsiOutputStream(stream, { 0 } as AnsiOutputStream.WidthSupplier,
                AnsiMode.Strip, null, AnsiType.Redirected, AnsiColors.Colors16, Charset.defaultCharset(),
                null, null, false)
        return new AnsiPrintStream(strip, true)
    }

    private static boolean isTerminal(final SystemStream which) {
        for (String provider : ["jni", "exec"]) {
            try {
                return TerminalProvider.load(provider).isSystemStream(which)
            } catch (Throwable ignored) {
                // provider not available on this platform
            }
        }
        return false
    }
}
