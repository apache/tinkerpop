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
package org.apache.tinkerpop.gremlin.console

import org.apache.tinkerpop.gremlin.console.commands.RemoteCommand
import org.apache.tinkerpop.gremlin.console.jsr223.AbstractGremlinServerIntegrationTest
import org.apache.tinkerpop.gremlin.console.jsr223.DriverRemoteAcceptor
import org.apache.tinkerpop.gremlin.console.jsr223.MockGroovyGremlinShellEnvironment
import org.codehaus.groovy.tools.shell.IO
import org.junit.Test

import java.nio.file.Paths

class GremlinGroovyshTest extends AbstractGremlinServerIntegrationTest {
    private IO testio
    private ByteArrayOutputStream out
    private ByteArrayOutputStream err
    private GremlinGroovysh shell

    @Override
    void setUp() {
        super.setUp()
        out = new ByteArrayOutputStream()
        err = new ByteArrayOutputStream()
        testio = new IO(new ByteArrayInputStream(), out, err)
        shell = new GremlinGroovysh(new Mediator(null), testio)
    }

    @Override
    void tearDown() {
        super.tearDown()
        shell.execute(":purge preferences") // for test cases where persistent preferences (interpreterMode) are set.
    }

    @Test
    void shouldEnableRemoteConsole() {
        setupRemote(shell)
        shell.execute(":remote console")

        assert (false == shell.mediator.localEvaluation)
        assert out.toString().startsWith("All scripts will now be sent to")
    }

    @Test
    void shouldGetSimpleResultFromRemoteConsole() {
        setupRemote(shell)
        shell.execute(":remote console")
        out.reset()
        shell.execute("1+1")

        assert ("2" == out.toString())
    }

    @Test
    void shouldGetGremlinResultFromRemoteConsole() {
        setupRemote(shell)
        shell.execute(":remote console")
        out.reset()
        shell.execute("g.V().count()")

        assert ("0" == out.toString())
    }

    @Test
    void shouldGetMultilineResultFromRemoteConsole() {
        setupRemote(shell)
        shell.execute(":remote console")
        out.reset()
        shell.execute("if (true) {")
        shell.execute("g.V().count() }")

        assert ("0" == out.toString())
    }

    @Test
    void shouldNotSubmitIncompleteLinesFromRemoteConsole() {
        setupRemote(shell)
        shell.execute(":remote console")
        shell.execute("if (0 == g.V().count()) {")

        assert (0 != shell.buffers.current().size())
    }

    @Test
    void shouldGetGremlinResultFromRemoteConsoleInInterpreterMode() {
        setupRemote(shell)
        shell.execute(":remote console")
        shell.execute(":set interpreterMode")
        out.reset()
        shell.execute("g.V().count()")

        assert ("0" == out.toString())
    }

    @Test
    void shouldGetMultilineResultFromRemoteConsoleInInterpreterMode() {
        setupRemote(shell)
        shell.execute(":remote console")
        shell.execute(":set interpreterMode")
        out.reset()
        shell.execute("if (true) {")
        shell.execute("g.V().count() }")

        assert ("0" == out.toString())
    }

    @Test
    void shouldOnlyExecuteOnceRemoteConsoleInInterpreterMode() {
        setupRemote(shell)
        shell.execute(":remote console")
        shell.execute(":set interpreterMode")
        out.reset()
        shell.execute("a = 1")

        assert "1" == out.toString()
    }

    private def setupRemote(GremlinGroovysh shell) {
        shell.setResultHook(handleResult)
        shell.register(new RemoteCommand(shell, shell.mediator))
        shell.mediator.addRemote(new DriverRemoteAcceptor(new MockGroovyGremlinShellEnvironment(shell)))
        shell.mediator.currentRemote().connect([Paths.get(AbstractGremlinServerIntegrationTest.class.getResource("remote.yaml").toURI()).toString()])

        server.getServerGremlinExecutor().getGremlinExecutor().getScriptEngineManager().put(
                "g",
                server.getServerGremlinExecutor().getGraphManager().getGraph("graph").traversal())
    }

    private def handleResult = { result ->
        if (result instanceof Iterator) {
            Iterator resultItr = (Iterator) result

            while (resultItr.hasNext()) {
                testio.out.print(resultItr.next())
                testio.out.flush()
            }
        } else if (result instanceof Number) {
            testio.out.print((Number) result)
            testio.out.flush()
        } else if (result instanceof String) {
            testio.out.print((String) result)
            testio.out.flush()
        }
    }
}
