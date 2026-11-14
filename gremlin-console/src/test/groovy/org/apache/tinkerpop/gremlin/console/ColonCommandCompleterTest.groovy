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

import org.jline.reader.Completer
import org.jline.reader.ParsedLine
import org.jline.reader.Parser
import org.jline.reader.impl.DefaultParser
import org.junit.Test

import java.util.function.Predicate

import static org.junit.Assert.assertEquals

class ColonCommandCompleterTest {

    private static final Predicate<String> PLUGIN_ONLY = { String c -> c == "/plugin" } as Predicate<String>

    /**
     * The words the wrapped completer sees for {@code line}.
     */
    private static List<String> wordsSeen(final String line) {
        List<String> seen = null
        final Completer delegate = { reader, ParsedLine l, candidates -> seen = l.words() } as Completer
        final ParsedLine parsed = new DefaultParser().parse(line, line.length(), Parser.ParseContext.COMPLETE)
        new ColonCommandCompleter(delegate, PLUGIN_ONLY).complete(null, parsed, [])
        return seen
    }

    @Test
    void shouldPresentColonCommandAsSlashCommand() {
        assertEquals(["/plugin", "use", ""], wordsSeen(":plugin use "))
    }

    @Test
    void shouldLeaveUnknownColonCommandAlone() {
        assertEquals([":nope", ""], wordsSeen(":nope "))
    }

    @Test
    void shouldLeaveCommandWordAlone() {
        assertEquals([":plu"], wordsSeen(":plu"))
    }

    @Test
    void shouldLeaveCodeAlone() {
        assertEquals(["x", "=", ""], wordsSeen("x = "))
    }
}
