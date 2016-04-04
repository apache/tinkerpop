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

package org.apache.tinkerpop.gremlin.hadoop.groovy.plugin;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import org.apache.tinkerpop.gremlin.groovy.util.SugarTestHelper;
import org.apache.tinkerpop.gremlin.groovy.util.TestableConsolePluginAcceptor;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.HadoopGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This is an test that is mean to be used in the context of the {@link HadoopGremlinSuite} and shouldn't be
 * executed on its own.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopGremlinPluginCheck extends AbstractGremlinTest {

    @Before
    public void setupTest() {
        try {
            this.console = new TestableConsolePluginAcceptor();
            final HadoopGremlinPlugin plugin = new HadoopGremlinPlugin();
            plugin.pluginTo(this.console);
            this.remote = (HadoopRemoteAcceptor) plugin.remoteAcceptor().get();
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    ///////////////////

    private HadoopRemoteAcceptor remote;
    private TestableConsolePluginAcceptor console;

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportRemoteTraversal() throws Exception {
        this.console.addBinding("graph", this.graph);
        this.console.addBinding("g", this.g);
        this.remote.connect(Arrays.asList("graph", "g"));
        //
        Traversal<?, ?> traversal = (Traversal<?, ?>) this.remote.submit(Arrays.asList("g.V().count()"));
        assertEquals(6L, traversal.next());
        assertFalse(traversal.hasNext());
        assertNotNull(this.console.getBindings().get(RemoteAcceptor.RESULT));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportRemoteSugarTraversal() throws Exception {
        SugarTestHelper.clearRegistry(this.graphProvider);
        this.console.addBinding("graph", this.graph);
        this.console.addBinding("g", this.g);
        this.remote.connect(Arrays.asList("graph"));
        //
        this.remote.configure(Arrays.asList("useSugar", "true"));
        this.remote.connect(Arrays.asList("graph", "g"));
        Traversal<?, ?> traversal = (Traversal<?, ?>) this.remote.submit(Arrays.asList("g.V.name.map{it.length()}.sum"));
        assertEquals(28l, traversal.next());
        assertFalse(traversal.hasNext());
        assertNotNull(this.console.getBindings().get(RemoteAcceptor.RESULT));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportHDFSMethods() throws Exception {
        List<String> ls = (List<String>) this.console.eval("hdfs.ls()");
        for (final String line : ls) {
            assertTrue(line.startsWith("-") || line.startsWith("r") || line.startsWith("w") || line.startsWith("x"));
            assertEquals(" ", line.substring(9, 10));
        }
        ls = (List<String>) this.console.eval("fs.ls()");
        for (final String line : ls) {
            assertTrue(line.startsWith("-") || line.startsWith("r") || line.startsWith("w") || line.startsWith("x"));
            assertEquals(" ", line.substring(9, 10));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldGracefullyHandleBadGremlinHadoopLibs() throws Exception {
        System.setProperty(Constants.HADOOP_GREMLIN_LIBS, TestHelper.makeTestDataDirectory(HadoopGremlinPluginCheck.class, "shouldGracefullyHandleBadGremlinHadoopLibs"));
        this.graph.configuration().setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true);
        this.console.addBinding("graph", this.graph);
        this.console.addBinding("g", this.g);
        this.remote.connect(Arrays.asList("graph", "g"));
        Traversal<?, ?> traversal = (Traversal<?, ?>) this.remote.submit(Arrays.asList("g.V()"));
        assertEquals(6, IteratorUtils.count(traversal));
        assertNotNull(this.console.getBindings().get(RemoteAcceptor.RESULT));
    }
}
