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

import org.apache.tinkerpop.gremlin.console.ConsoleFs
import org.apache.tinkerpop.gremlin.console.GremlinGroovysh
import org.apache.tinkerpop.gremlin.console.Mediator
import org.codehaus.groovy.tools.shell.IO
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.assertTrue

class UninstallCommandTest {
    private UninstallCommand cmd

    @Before
    void setUp() {
        def io = new IO(new ByteArrayInputStream(), new ByteArrayOutputStream(), new ByteArrayOutputStream())
        def shell = new GremlinGroovysh(new Mediator(null), io)
        cmd = new UninstallCommand(shell, new Mediator(null))
    }

    @Test
    void shouldReportMissingModuleArgument() {
        assertTrue(((String) cmd.execute([])).contains("Specify the name"))
    }

    @Test
    void shouldReportUnknownModule() {
        def result = (String) cmd.execute(["no-such-module-xyz123"])
        assertTrue(result.contains("no module with the name"))
    }

    @Test
    void shouldUninstallExistingModule() {
        def name = "test-uninstall-" + System.currentTimeMillis()
        def dir = new File(ConsoleFs.CONSOLE_HOME_DIR, name)
        dir.mkdirs()
        try {
            def result = (String) cmd.execute([name])
            assertTrue(result.contains("Uninstalled"))
            assertTrue(!dir.exists())
        } finally {
            dir.deleteDir()
        }
    }
}
