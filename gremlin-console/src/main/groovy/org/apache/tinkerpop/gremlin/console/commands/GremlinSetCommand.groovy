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
package org.apache.tinkerpop.gremlin.console.commands

import jline.console.completer.Completer

import org.apache.groovy.groovysh.Groovysh
import org.apache.groovy.groovysh.commands.SetCommand
import org.apache.groovy.groovysh.util.PackageHelper
import org.codehaus.groovy.tools.shell.util.Preferences
import org.codehaus.groovy.tools.shell.util.SimpleCompletor

/**
 * A Gremlin-specific implementation of the {@code SetCommand} provided by Groovy.  Didn't see another way to
 * get auto-complete features for Gremlin-specific things to be added to preferences so a subclass with an override
 * seemed to be the only way.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinSetCommand extends SetCommand {

    GremlinSetCommand(final Groovysh shell) {
        super(shell)
    }

    @Override
    protected List<Completer> createCompleters() {
        def loader = {
            final Set<String> set = [] as Set<String>

            final String[] keys = Preferences.keys()

            keys.each { String key -> set.add(key) }

            set << Preferences.VERBOSITY_KEY
            set << Preferences.EDITOR_KEY
            set << Preferences.PARSER_FLAVOR_KEY
            set << Preferences.SANITIZE_STACK_TRACE_KEY
            set << Preferences.SHOW_LAST_RESULT_KEY
            set << Groovysh.INTERPRETER_MODE_PREFERENCE_KEY
            set << Groovysh.AUTOINDENT_PREFERENCE_KEY
            set << Groovysh.COLORS_PREFERENCE_KEY
            set << Groovysh.METACLASS_COMPLETION_PREFIX_LENGTH_PREFERENCE_KEY
            set << PackageHelper.IMPORT_COMPLETION_PREFERENCE_KEY

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

        return [
                new SimpleCompletor(loader),
                null
        ]
    }
}
