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

import org.jline.reader.Completer
import org.jline.reader.impl.completer.StringsCompleter

/**
 * A Gremlin-specific implementation of the {@code SetCommand} provided by Groovy 4, which Groovy 6 no longer has, so
 * the setting and listing of preferences is done here as well.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinSetCommand extends GremlinCommand {

    private static final java.util.prefs.Preferences STORE =
            java.util.prefs.Preferences.userRoot().node("/org/apache/tinkerpop/gremlin/console")

    GremlinSetCommand() {
        super("/set")
    }

    @Override
    String description() { "Set (or list) preferences" }

    @Override
    String usage() { "[<name> [<value>]]" }

    @Override
    String help() { "Set or list preferences." }

    @Override
    List<Completer> completers() {
        def loader = {
            final Set<String> set = [] as Set<String>
            final String[] keys = STORE.keys()
            keys.each { String key -> set.add(key) }
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_COLORS
            // add Gremlin Console specific preferences here
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREFERENCE_ITERATION_MAX
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_GREMLIN_COLOR
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_ERROR_COLOR
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_INFO_COLOR
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_INPUT_PROMPT_COLOR
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_RESULT_PROMPT_COLOR
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_RESULT_IND_NULL
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_INPUT_PROMPT
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_RESULT_PROMPT
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_EDGE_COLOR
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_VERTEX_COLOR
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_STRING_COLOR
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_NUMBER_COLOR
            set << org.apache.tinkerpop.gremlin.console.Preferences.PREF_T_COLOR
            return set.toList()
        }

        return [COMMAND_WORD, new StringsCompleter(loader as java.util.function.Supplier)]
    }

    /**
     * The same as groovysh 4's {@code SetCommand}.
     */
    @Override
    Object execute(final List<String> args) {
        assert args != null

        if (args.size() == 0) {
            def keys = STORE.keys()

            if (keys.size() == 0) {
                out.println('No preferences are set')
                return
            }

            out.println('Preferences:')
            keys.each { String key ->
                def keyvalue = STORE.get(key, null)
                out.println("    $key=$keyvalue")
            }
            return
        }

        if (args.size() > 2) {
            fail("Command ':set' requires arguments: <name> [<value>]")
        }

        String name = args[0]
        def value

        if (args.size() == 1) {
            value = true
        } else {
            value = args[1]
        }

        STORE.put(name, String.valueOf(value))
    }
}
