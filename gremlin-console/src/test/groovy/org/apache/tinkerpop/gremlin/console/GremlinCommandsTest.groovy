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

import org.apache.groovy.groovysh.jline.GroovyEngine
import org.jline.builtins.Options.HelpException
import org.jline.shell.CommandSession
import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

/**
 * The set of commands the console registers with groovysh.
 */
class GremlinCommandsTest {

    private final def commands = GremlinCommands.create(new Mediator(null), new GroovyEngine()).commands()

    @Test
    void shouldNameCommandsWithTheirShortcuts() {
        assertEquals(["/plugin", "/set", "/install", "/uninstall", "/cls"], commands.collect { it.name() })
        assertEquals(["/pin"], commands.find { it.name() == "/plugin" }.aliases())
        assertEquals(["/C"], commands.find { it.name() == "/cls" }.aliases())
    }

    @Test
    void shouldAnswerHelpForEveryCommand() {
        final CommandSession session = new CommandSession(null, new ByteArrayInputStream(new byte[0]),
                new PrintStream(new ByteArrayOutputStream(), true), new PrintStream(new ByteArrayOutputStream(), true))
        commands.each { command ->
            ["--help", "-?"].each { flag ->
                try {
                    command.execute(session, [flag] as String[])
                    fail("${command.name()} $flag should have thrown HelpException")
                } catch (HelpException e) {
                    final String help = e.message
                    assertTrue(help, help.startsWith(command.name().substring(1) + " -  " + command.description()))
                    assertTrue(help, help.contains("Usage: " + command.name().substring(1)))
                }
            }
        }
    }
}
