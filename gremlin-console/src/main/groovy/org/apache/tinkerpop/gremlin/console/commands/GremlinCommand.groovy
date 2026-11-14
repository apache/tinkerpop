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

import org.jline.builtins.Options.HelpException
import org.jline.reader.Candidate
import org.jline.reader.Completer
import org.jline.shell.CommandSession
import org.jline.shell.impl.AbstractCommand

/**
 * Takes the place of groovysh 4's {@code CommandSupport}: commands implement {@link #execute(List)}, call
 * {@link #fail(String)} for bad arguments and answer {@code --help} and {@code -?}, which is how
 * {@code :help <command>} asks for help.
 */
abstract class GremlinCommand extends AbstractCommand {

    /**
     * Matches whatever command name or alias was typed. Argument completers start with the command word, so this
     * fills that position.
     */
    static final Completer COMMAND_WORD = { reader, line, candidates -> candidates << new Candidate(line.word()) } as Completer

    protected PrintStream out = System.out

    GremlinCommand(final String name, final String... aliases) {
        super(name, aliases)
    }

    /**
     * The arguments the command takes, e.g. {@code <module>}.
     */
    abstract String usage()

    String help() { description() }

    String helpText() {
        final String bare = name().substring(1)
        return "$bare -  ${description()}\n" +
                "Usage: $bare ${usage()}".trim() + "\n" +
                "  -? --help                       Displays command help\n" +
                help()
    }

    abstract Object execute(final List<String> arguments)

    @Override
    Object execute(final CommandSession session, final String[] args) {
        if (args.length == 1 && (args[0] == "--help" || args[0] == "-?")) throw new HelpException(helpText())
        // the line reader keeps the quote of an argument that is not closed
        final String unclosed = args.find { it.length() > 0 && (it[0] == '"' || it[0] == "'") && !(it.length() > 1 && it.endsWith(it[0])) }
        if (unclosed != null) throw new IllegalArgumentException("Missing closing " + unclosed[0])
        out = session.out()
        return execute(args as List<String>)
    }

    /**
     * Rejects the arguments with a message the console prints without offering a stack trace.
     */
    protected static void fail(final String message) {
        throw new UsageException(message)
    }
}
