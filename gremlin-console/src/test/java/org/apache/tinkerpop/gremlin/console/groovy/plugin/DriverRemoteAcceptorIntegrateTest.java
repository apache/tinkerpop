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
package org.apache.tinkerpop.gremlin.console.groovy.plugin;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverRemoteAcceptorIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private final Groovysh groovysh = new Groovysh();
    private DriverRemoteAcceptor acceptor;

    @Rule
    public TestName name = new TestName();

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        try {
            final String tinkerGraphConfig = TestHelper.generateTempFileFromResource(this.getClass(), "tinkergraph-empty.properties", ".tmp").getAbsolutePath();
            settings.graphs.put("g", tinkerGraphConfig);
            return settings;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Before
    public void before() throws Exception {
        acceptor = new DriverRemoteAcceptor(groovysh);
    }

    @After
    public void after() {
        try {
            acceptor.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void shouldConnect() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
    }

    @Test
    public void shouldConnectAndSubmitSimple() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
        assertEquals("2", ((Iterator) acceptor.submit(Arrays.asList("1+1"))).next());
        assertEquals("2", ((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).iterator().next().getString());
    }

    @Test
    public void shouldConnectAndSubmitSimpleList() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
        assertThat(IteratorUtils.list(((Iterator<String>) acceptor.submit(Arrays.asList("[1,2,3,4,5]")))), contains("1", "2", "3", "4", "5"));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getString).collect(Collectors.toList()), contains("1", "2", "3", "4", "5"));
    }

    @Test
    public void shouldConnectAndReturnVertices() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
        assertThat(IteratorUtils.list(((Iterator<String>) acceptor.submit(Arrays.asList("g.addVertex('name','stephen');g.addVertex('name','marko');g.traversal().V()")))), hasSize(2));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getString).collect(Collectors.toList()), hasSize(2));
    }

    @Test
    public void shouldConnectAndReturnVerticesWithAnAlias() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
        acceptor.configure(Arrays.asList("alias", "x", "g"));
        assertThat(IteratorUtils.list(((Iterator<String>) acceptor.submit(Arrays.asList("x.addVertex('name','stephen');x.addVertex('name','marko');x.traversal().V()")))), hasSize(2));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getString).collect(Collectors.toList()), hasSize(2));
    }

    @Test
    public void shouldConnectAndSubmitForNull() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
        assertThat(IteratorUtils.list(((Iterator<String>) acceptor.submit(Arrays.asList("g.traversal().V().drop().iterate();null")))), contains("null"));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getObject).collect(Collectors.toList()), contains("null"));
    }
}
