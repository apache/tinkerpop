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

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.jsr223.console.GremlinShellEnvironment;
import org.apache.tinkerpop.gremlin.jsr223.console.RemoteException;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
            final String tinkerGraphConfig = Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "tinkergraph-empty.properties", ".tmp"));
            settings.graphs.put("g", tinkerGraphConfig);
            return settings;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Before
    public void before() throws Exception {
        final GremlinShellEnvironment env = new MockGroovyGremlinShellEnvironment(groovysh);
        acceptor = new DriverRemoteAcceptor(env);
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
    public void shouldConnectWithRemoteYaml() throws Exception {
        assertThat(acceptor.connect(Collections.singletonList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")))).toString(), startsWith("Configured "));
    }

    @Test
    public void shouldConnectWithRemoteVariable() throws Exception {
        groovysh.getInterp().evaluate(Collections.singletonList("cluster = " + Cluster.class.getName() + ".open(\"" + Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")) + "\")"));
        assertThat(acceptor.connect(Collections.singletonList("cluster")).toString(), startsWith("Configured "));
    }

    @Test
    public void shouldConnectAndSubmitSession() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")), "session")).toString(), startsWith("Configured "));
        assertEquals("1", ((Iterator) acceptor.submit(Collections.singletonList("x = 1"))).next());
        assertEquals("0", ((Iterator) acceptor.submit(Collections.singletonList("x - 1"))).next());
        assertEquals("0", ((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).iterator().next().getString());
    }

    @Test
    public void shouldConnectAndSubmitManagedSession() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")), "session-managed")).toString(), startsWith("Configured "));
        assertEquals("1", ((Iterator) acceptor.submit(Collections.singletonList("x = 1"))).next());
        assertEquals("0", ((Iterator) acceptor.submit(Collections.singletonList("x - 1"))).next());
        assertEquals("0", ((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).iterator().next().getString());
    }

    @Test
    public void shouldConnectAndSubmitSimple() throws Exception {
        assertThat(acceptor.connect(Collections.singletonList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")))).toString(), startsWith("Configured "));
        assertEquals("2", ((Iterator) acceptor.submit(Collections.singletonList("1+1"))).next());
        assertEquals("2", ((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).iterator().next().getString());
    }

    @Test
    public void shouldConnectAndSubmitSimpleList() throws Exception {
        assertThat(acceptor.connect(Collections.singletonList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")))).toString(), startsWith("Configured "));
        assertThat(IteratorUtils.list(((Iterator<String>) acceptor.submit(Collections.singletonList("[1,2,3,4,5]")))), contains("1", "2", "3", "4", "5"));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getString).collect(Collectors.toList()), contains("1", "2", "3", "4", "5"));
    }

    @Test
    public void shouldConnectAndReturnVertices() throws Exception {
        assertThat(acceptor.connect(Collections.singletonList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")))).toString(), startsWith("Configured "));
        assertThat(IteratorUtils.list(((Iterator<String>) acceptor.submit(Collections.singletonList("g.addVertex('name','stephen');g.addVertex('name','marko');g.traversal().V()")))), hasSize(2));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getString).collect(Collectors.toList()), hasSize(2));
    }

    @Test
    public void shouldConnectAndReturnVerticesWithAnAlias() throws Exception {
        assertThat(acceptor.connect(Collections.singletonList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")))).toString(), startsWith("Configured "));
        acceptor.configure(Arrays.asList("alias", "x", "g"));
        assertThat(IteratorUtils.list(((Iterator<String>) acceptor.submit(Collections.singletonList("x.addVertex('name','stephen');x.addVertex('name','marko');x.traversal().V()")))), hasSize(2));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getString).collect(Collectors.toList()), hasSize(2));
    }

    @Test
    public void shouldConnectAndSubmitForNull() throws Exception {
        assertThat(acceptor.connect(Collections.singletonList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")))).toString(), startsWith("Configured "));
        assertThat(IteratorUtils.list(((Iterator<String>) acceptor.submit(Collections.singletonList("g.traversal().V().drop().iterate();null")))), contains("null"));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getObject).collect(Collectors.toList()), contains("null"));
    }

    @Test
    public void shouldConnectAndSubmitInSession() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")), "session")).toString(), startsWith("Configured "));
        assertEquals("2", ((Iterator) acceptor.submit(Collections.singletonList("x=1+1"))).next());
        assertEquals("2", ((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).iterator().next().getString());
        assertEquals("4", ((Iterator) acceptor.submit(Collections.singletonList("x+2"))).next());
        assertEquals("4", ((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).iterator().next().getString());
    }

    @Test
    public void shouldConnectAndSubmitInNamedSession() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")), "session", "AAA")).toString(), startsWith("Configured "));
        assertEquals("2", ((Iterator) acceptor.submit(Collections.singletonList("x=1+1"))).next());
        assertEquals("2", ((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).iterator().next().getString());
        assertEquals("4", ((Iterator) acceptor.submit(Collections.singletonList("x+2"))).next());
        assertEquals("4", ((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).iterator().next().getString());
    }

    @Test
    public void shouldConnectAndSubmitWithTimeout() throws Exception {
        assertThat(acceptor.connect(Collections.singletonList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp")))).toString(), startsWith("Configured "));
        try {
            acceptor.submit(Collections.singletonList(String.format("g.with(%s, 10).inject(0).sideEffect{Thread.sleep(10000)}", Tokens.ARGS_EVAL_TIMEOUT)));
            fail("Request should have timed out");
        } catch (RemoteException re) {
            assertThat(re.getMessage(), containsString("evaluationTimeout"));
        }
    }
}
