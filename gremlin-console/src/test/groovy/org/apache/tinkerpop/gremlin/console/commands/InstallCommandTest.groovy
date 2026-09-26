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

import org.apache.tinkerpop.gremlin.console.GremlinGroovysh
import org.apache.tinkerpop.gremlin.console.GremlinGroovysh
import org.apache.tinkerpop.gremlin.console.Mediator
import groovy.grape.Grape
import org.junit.After
import org.codehaus.groovy.tools.shell.IO
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

class InstallCommandTest {
    private InstallCommand cmd

    @Before
    void setUp() {
        def io = new IO(new ByteArrayInputStream(), new ByteArrayOutputStream(), new ByteArrayOutputStream())
        def shell = new GremlinGroovysh(new Mediator(null), io)
        cmd = new InstallCommand(shell, new Mediator(null))
    }

    @Test(expected = IllegalArgumentException)
    void shouldRejectMissingGroup() { cmd.execute([]) }

    @Test(expected = IllegalArgumentException)
    void shouldRejectMissingModule() { cmd.execute(["org.example"]) }

    @Test(expected = IllegalArgumentException)
    void shouldRejectMissingVersion() { cmd.execute(["org.example", "some-artifact"]) }

    @Test
    void shouldReturnErrorMessageWhenArtifactCannotBeResolved() {
        // valid args but a bogus, unresolvable artifact -> download fails -> catch returns the message (String)
        def result = cmd.execute(["com.tinkerpop.nonexistent.fake", "no-such-artifact-xyz", "9.9.9"])
        assertNotNull(result)
        assertTrue(result instanceof String)
        final String message = (String) result
        // the returned message reports the dependency-resolution failure for the requested artifact
        assertTrue(message, message.contains("unresolved dependency"))
        assertTrue(message, message.contains("no-such-artifact-xyz"))
    }

    @After
    void tearDown() {
        // remove the metaclass stub so other tests/JVM state are unaffected
        GroovySystem.metaClassRegistry.removeMetaClass(Grape)
    }

    @Test
    void shouldRegisterAvailablePluginsFromServiceLoaderInGrabDeps() {
        // stub the network-dependent Grape.grab so grabDeps proceeds offline;
        // ServiceLoader then finds the GremlinPlugin impls already on the classpath and runs the closure body
        Grape.metaClass.static.grab = { Map m -> null }
        def io = new IO(new ByteArrayInputStream(), new ByteArrayOutputStream(), new ByteArrayOutputStream())
        def mediator = new Mediator(null)
        def shell = new GremlinGroovysh(mediator, io)
        def cmd = new InstallCommand(shell, mediator)
        cmd.grabDeps([group: "x", module: "y", version: "z"])
        assertTrue(mediator.availablePlugins.size() > 0)
    }
}
