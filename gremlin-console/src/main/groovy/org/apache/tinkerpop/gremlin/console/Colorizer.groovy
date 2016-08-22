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

package org.apache.tinkerpop.gremlin.console;

import org.codehaus.groovy.tools.shell.AnsiDetector
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.AnsiConsole

public class Colorizer {

    public static void installAnsi() {
        // must be called before IO(), since it modifies System.in
        // Install the system adapters, replaces System.out and System.err
        // Must be called before using IO(), because IO stores refs to System.out and System.err
        AnsiConsole.systemInstall()
        // Register jline ansi detector
        Ansi.setDetector(new AnsiDetector())
        Ansi.enabled = true
    }

    public static String render(String color, String text) {
        if (Ansi.isEnabled() && Preferences.colors) {
            Ansi.ansi().render(String.format("@|%s %s|@", color, text)).toString()
        } else {
            return text
        }
    }
}
