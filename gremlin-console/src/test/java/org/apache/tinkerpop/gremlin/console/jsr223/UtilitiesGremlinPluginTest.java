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
package org.apache.tinkerpop.gremlin.console.jsr223;

import org.apache.commons.io.input.NullInputStream;
import org.apache.groovy.groovysh.Groovysh;
import org.apache.tinkerpop.gremlin.console.PluggedIn;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.codehaus.groovy.tools.shell.IO;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class UtilitiesGremlinPluginTest {
    private final InputStream inputStream = new NullInputStream(0);
    private final OutputStream outputStream = new ByteArrayOutputStream();
    private final OutputStream errorStream = new ByteArrayOutputStream();
    private final IO io = new IO(inputStream, outputStream, errorStream);

    @Test
    public void shouldFailWithoutUtilitiesPlugin() {
        final Groovysh groovysh = new Groovysh();
        try {
            groovysh.execute("describeGraph(g.class)");
            fail("Utilities were not loaded - this should fail.");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void shouldPluginUtilities() {
        final UtilitiesGremlinPlugin plugin = new UtilitiesGremlinPlugin();

        final Groovysh groovysh = new Groovysh();
        groovysh.getInterp().getContext().setProperty("g", TinkerFactory.createClassic());

        final PluggedIn pluggedIn = new PluggedIn(plugin, groovysh, io, false);
        pluggedIn.activate();

        assertThat(groovysh.execute("describeGraph(org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph)").toString(), containsString("IMPLEMENTATION - org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph"));
    }
}
