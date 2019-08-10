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
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphFactoryTest {
    @Test
    public void shouldBeUtilityClass() throws Exception {
        TestHelper.assertIsUtilityClass(GraphFactory.class);
    }

    @Test
    public void shouldThrowExceptionOnNullConfiguration() {
        try {
            GraphFactory.open((Configuration) null);
            fail("Should have thrown an exception since configuration is null");
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.argumentCanNotBeNull("configuration");
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionOnNullMapConfiguration() {
        try {
            GraphFactory.open((Map) null);
            fail("Should have thrown an exception since configuration is null");
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.argumentCanNotBeNull("configuration");
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionOnNullFileConfiguration() {
        try {
            GraphFactory.open((String) null);
            fail("Should have thrown an exception since configuration is null");
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.argumentCanNotBeNull("configurationFile");
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionOnIfConfigurationFileIsNotAnActualFile() {
        GraphFactory.open(TestHelper.makeTestDataDirectory(GraphFactoryTest.class, "path"));
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfGraphKeyIsNotPresentInConfiguration() {
        final Configuration conf = new BaseConfiguration();
        GraphFactory.open(conf);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfGraphKeyIsNotPresentInMapConfig() {
        final Map<String,Object> conf = new HashMap<>();
        GraphFactory.open(conf);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfGraphKeyIsNotValidClassInConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, "not.a.real.class.that.is.a.SomeGraph");
        GraphFactory.open(conf);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfGraphKeyIsNotValidClassInMapConfig() {
        final Map<String,Object> conf = new HashMap<>();
        conf.put(Graph.GRAPH, "not.a.real.class.that.is.a.SomeGraph");
        GraphFactory.open(conf);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfGraphDoesNotHaveOpenMethodViaConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, MockGraphWithoutOpen.class.getName());
        GraphFactory.open(conf);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfGraphDoesNotHaveOpenMethodViaMapConfig() {
        final Map<String,Object> conf = new HashMap<>();
        conf.put(Graph.GRAPH, MockGraphWithoutOpen.class.getName());
        GraphFactory.open(conf);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfGraphThrowsWhileConfiguringViaConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, MockGraph.class.getName());
        conf.setProperty("throw", "it");
        GraphFactory.open(conf);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfGraphThrowsWhileConfiguringViaMapConfig() {
        final Map<String,Object> conf = new HashMap<>();
        conf.put(Graph.GRAPH, MockGraph.class.getName());
        conf.put("throw", "it");
        GraphFactory.open(conf);
    }

    @Test
    public void shouldOpenViaConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, MockGraph.class.getName());
        conf.setProperty("keep", "it");
        GraphFactory.open(conf);
        final MockGraph g = (MockGraph) GraphFactory.open(conf);
        assertEquals(MockGraph.class.getName(), g.getConf().getString(Graph.GRAPH));
        assertEquals("it", g.getConf().getString("keep"));
    }

    @Test
    public void shouldOpenViaMapConfig() {
        final Map<String,Object> conf = new HashMap<>();
        conf.put(Graph.GRAPH, MockGraph.class.getName());
        conf.put("keep", "it");
        final MockGraph g = (MockGraph) GraphFactory.open(conf);
        assertEquals(MockGraph.class.getName(), g.getConf().getString(Graph.GRAPH));
        assertEquals("it", g.getConf().getString("keep"));
    }

    @Test
    public void shouldOpenViaPropertiesFileConfig() throws IOException {
        final File confFile = TestHelper.generateTempFileFromResource(GraphFactoryTest.class, "mockgraph.properties", ".properties");
        final MockGraph g = (MockGraph) GraphFactory.open(confFile.getAbsolutePath());
        assertEquals(MockGraph.class.getName(), g.getConf().getString(Graph.GRAPH));
        assertEquals("it", g.getConf().getString("keep"));
    }

    @Test
    public void shouldOpenViaPropertiesFileConfigAsDefault() throws IOException {
        final File confFile = TestHelper.generateTempFileFromResource(GraphFactoryTest.class, "mockgraph.properties", ".notrecognized");
        final MockGraph g = (MockGraph) GraphFactory.open(confFile.getAbsolutePath());
        assertEquals(MockGraph.class.getName(), g.getConf().getString(Graph.GRAPH));
        assertEquals("it", g.getConf().getString("keep"));
    }

    @Test
    public void shouldOpenViaXmlFileConfig() throws IOException {
        final File confFile = TestHelper.generateTempFileFromResource(GraphFactoryTest.class, "mockgraph.xml", ".xml");
        final MockGraph g = (MockGraph) GraphFactory.open(confFile.getAbsolutePath());
        assertEquals(MockGraph.class.getName(), g.getConf().getString(Graph.GRAPH));
        assertEquals("it", g.getConf().getString("keep"));
    }

    @Test
    public void shouldOpenViaYamlFileConfig() throws IOException {
        final File confFile = TestHelper.generateTempFileFromResource(GraphFactoryTest.class, "mockgraph.yaml", ".yaml");
        final MockGraph g = (MockGraph) GraphFactory.open(confFile.getAbsolutePath());
        assertEquals(MockGraph.class.getName(), g.getConf().getString(Graph.GRAPH));
        assertEquals("it", g.getConf().getString("keep"));
    }

    @Test
    public void shouldOpenViaYmlFileConfig() throws IOException {
        final File confFile = TestHelper.generateTempFileFromResource(GraphFactoryTest.class, "mockgraph.yaml", ".yml");
        final MockGraph g = (MockGraph) GraphFactory.open(confFile.getAbsolutePath());
        assertEquals(MockGraph.class.getName(), g.getConf().getString(Graph.GRAPH));
        assertEquals("it", g.getConf().getString("keep"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidConfigTypeViaFileConfig() throws IOException {
        final File confFile = TestHelper.generateTempFileFromResource(GraphFactoryTest.class, "mockgraph-busted.yaml", ".yaml");
        final MockGraph g = (MockGraph) GraphFactory.open(confFile.getAbsolutePath());
        assertEquals(MockGraph.class.getName(), g.getConf().getString(Graph.GRAPH));
        assertEquals("it", g.getConf().getString("keep"));
    }

    @Test
    public void shouldOpenWithFactoryViaConcreteClass() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, MockGraphWithFactory.class.getName());
        GraphFactory.open(conf);
        final MockGraphWithFactory g = (MockGraphWithFactory) GraphFactory.open(conf);
        assertEquals(conf, g.getConf());
    }


    @Test
    public void shouldOpenWithFactoryViaConcreteInterface() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, MockGraphInterface.class.getName());
        GraphFactory.open(conf);
        final MockGraphInterface g = (MockGraphInterface) GraphFactory.open(conf);
        assertEquals(conf, g.getConf());
    }


    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfFactoryDoesNotHaveOpenMethodViaConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, MockGraphWithFactoryWithoutOpen.class.getName());
        GraphFactory.open(conf);
    }

    @Test
    public void shouldCreateAnEmptyGraphInstance() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, EmptyGraph.class.getName());
        final Graph graph = GraphFactory.open(conf);
        assertSame(EmptyGraph.instance(), graph);
    }

    public static class MockGraphWithoutOpen implements Graph {
        @Override
        public Vertex addVertex(final Object... keyValues) {
            return null;
        }

        @Override
        public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
            return null;
        }

        @Override
        public GraphComputer compute() throws IllegalArgumentException {
            return null;
        }

        @Override
        public Iterator<Vertex> vertices(final Object... vertexIds) {
            return null;
        }

        @Override
        public Iterator<Edge> edges(final Object... edgeIds) {
            return null;
        }

        @Override
        public Transaction tx() {
            return null;
        }

        @Override
        public Variables variables() {
            return null;
        }

        @Override
        public Configuration configuration() {
            return null;
        }

        @Override
        public void close() throws Exception {

        }
    }

    public static class MockGraph implements Graph {

        private final Configuration conf;
        MockGraph(final Configuration conf) {
            this.conf = conf;
        }

        public Configuration getConf() {
            return conf;
        }

        public static Graph open(final Configuration conf) {
            if (conf.containsKey("throw")) throw new RuntimeException("you said to throw me");
            return new MockGraph(conf);
        }

        @Override
        public Vertex addVertex(final Object... keyValues) {
            return null;
        }

        @Override
        public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
            return null;
        }

        @Override
        public GraphComputer compute() throws IllegalArgumentException {
            return null;
        }

        @Override
        public Iterator<Vertex> vertices(final Object... vertexIds) {
            return null;
        }

        @Override
        public Iterator<Edge> edges(final Object... edgeIds) {
            return null;
        }

        @Override
        public Transaction tx() {
            return null;
        }

        @Override
        public Variables variables() {
            return null;
        }

        @Override
        public Configuration configuration() {
            return null;
        }

        @Override
        public void close() throws Exception {

        }
    }

    public static class MockFactory {
        public static Graph open(final Configuration conf) {
            if (conf.containsKey("throw")) throw new RuntimeException("you said to throw me");
            return new MockGraphWithFactory(conf);
        }
    }

    @GraphFactoryClass(MockFactory.class)
    public static class MockGraphWithFactory implements MockGraphInterface {

        private final Configuration conf;

        MockGraphWithFactory(final Configuration conf) {
            this.conf = conf;
        }

        public Configuration getConf() {
            return conf;
        }

        @Override
        public Vertex addVertex(Object... keyValues) {
            return null;
        }

        @Override
        public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
            return null;
        }

        @Override
        public GraphComputer compute() throws IllegalArgumentException {
            return null;
        }

        @Override
        public Iterator<Vertex> vertices(Object... vertexIds) {
            return null;
        }

        @Override
        public Iterator<Edge> edges(Object... edgeIds) {
            return null;
        }

        @Override
        public Transaction tx() {
            return null;
        }

        @Override
        public Variables variables() {
            return null;
        }

        @Override
        public Configuration configuration() {
            return null;
        }

        @Override
        public void close() throws Exception {

        }
    }


    public static class MockFactoryWithOutOpen {

    }

    @GraphFactoryClass(MockFactoryWithOutOpen.class)
    public static class MockGraphWithFactoryWithoutOpen implements Graph {

        private final Configuration conf;

        MockGraphWithFactoryWithoutOpen(final Configuration conf) {
            this.conf = conf;
        }

        public Configuration getConf() {
            return conf;
        }

        @Override
        public Vertex addVertex(Object... keyValues) {
            return null;
        }

        @Override
        public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
            return null;
        }

        @Override
        public GraphComputer compute() throws IllegalArgumentException {
            return null;
        }

        @Override
        public Iterator<Vertex> vertices(Object... vertexIds) {
            return null;
        }

        @Override
        public Iterator<Edge> edges(Object... edgeIds) {
            return null;
        }

        @Override
        public Transaction tx() {
            return null;
        }

        @Override
        public Variables variables() {
            return null;
        }

        @Override
        public Configuration configuration() {
            return null;
        }

        @Override
        public void close() throws Exception {

        }
    }

    @GraphFactoryClass(MockFactory.class)
    public interface MockGraphInterface extends Graph {
        Configuration getConf();
    }
}
