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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.io.input.NullInputStream;
import org.apache.tinkerpop.gremlin.console.GremlinGroovysh;
import org.apache.tinkerpop.gremlin.console.Mediator;
import org.apache.tinkerpop.gremlin.jsr223.console.RemoteException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GephiRemoteAcceptorIntegrateTest {
    private static final Groovysh groovysh = new GremlinGroovysh(new Mediator(null), new IO());
    private static int port = pickOpenPort();

    private GephiRemoteAcceptor acceptor;

    private final InputStream inputStream = new NullInputStream(0);
    private final OutputStream outputStream = new ByteArrayOutputStream();
    private final OutputStream errorStream = new ByteArrayOutputStream();
    private final IO io = new IO(inputStream, outputStream, errorStream);

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(port);

    static {
        final Graph graph = TinkerFactory.createModern();
        groovysh.getInterp().getContext().setProperty("graph", graph);
    }

    @Before
    public void before() throws Exception {
        acceptor = new GephiRemoteAcceptor(new MockGroovyGremlinShellEnvironment(groovysh, io));
        acceptor.configure(Arrays.asList("port", String.valueOf(port)));
    }

    @Test
    public void shouldConnectWithDefaults() throws RemoteException {
        assertThat(acceptor.connect(Collections.emptyList()).toString(), startsWith("Connection to Gephi - http://localhost:" + port + "/workspace1"));
    }

    @Test
    public void shouldSubmitGraph() throws RemoteException {
        stubFor(post(urlPathEqualTo("/workspace1"))
                .withQueryParam("format", equalTo("JSON"))
                .withQueryParam("operation", equalTo("updateGraph"))
                .willReturn(aResponse()
                        .withStatus(200)));

        stubFor(get(urlPathEqualTo("/workspace1"))
                .withQueryParam("operation", equalTo("getNode"))
                .willReturn(aResponse()
                        .withStatus(200)));

        acceptor.submit(Arrays.asList("g = org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph.open();g.addVertex();g"));

        wireMockRule.verify(4, postRequestedFor(urlPathEqualTo("/workspace1")));
        wireMockRule.verify(1, getRequestedFor(urlPathEqualTo("/workspace1")));
    }

    @Test
    public void shouldSubmitTraversalAfterConfigWithDefaultG() throws RemoteException {
        stubFor(post(urlPathEqualTo("/workspace1"))
                .withQueryParam("format", equalTo("JSON"))
                .withQueryParam("operation", equalTo("updateGraph"))
                .willReturn(aResponse()
                        .withStatus(200)));

        acceptor.configure(Arrays.asList("visualTraversal", "graph"));

        // call iterate() as groovysh isn't rigged to auto-iterate
        acceptor.submit(Arrays.asList(
                "vg.V(2).in('knows').out('knows').has('age',org.apache.tinkerpop.gremlin.process.traversal.P.gt(30)).outE('created').has('weight',org.apache.tinkerpop.gremlin.process.traversal.P.gt(0.5d)).inV().iterate()"));

        wireMockRule.verify(18, postRequestedFor(urlPathEqualTo("/workspace1")));
    }

    @Test
    public void shouldSubmitTraversalAfterConfigWithDefinedG() throws RemoteException {
        stubFor(post(urlPathEqualTo("/workspace1"))
                .withQueryParam("format", equalTo("JSON"))
                .withQueryParam("operation", equalTo("updateGraph"))
                .willReturn(aResponse()
                        .withStatus(200)));

        acceptor.configure(Arrays.asList("visualTraversal", "graph", "x"));

        // call iterate() as groovysh isn't rigged to auto-iterate
        acceptor.submit(Arrays.asList(
                "x.V(2).in('knows').out('knows').has('age',org.apache.tinkerpop.gremlin.process.traversal.P.gt(30)).outE('created').has('weight',org.apache.tinkerpop.gremlin.process.traversal.P.gt(0.5d)).inV().iterate()"));

        wireMockRule.verify(18, postRequestedFor(urlPathEqualTo("/workspace1")));
    }

    @Test
    public void shouldSubmitTraversalOverRepeat() throws RemoteException {
        stubFor(post(urlPathEqualTo("/workspace1"))
                .withQueryParam("format", equalTo("JSON"))
                .withQueryParam("operation", equalTo("updateGraph"))
                .willReturn(aResponse()
                        .withStatus(200)));

        acceptor.configure(Arrays.asList("visualTraversal", "graph"));

        // call iterate() as groovysh isn't rigged to auto-iterate
        acceptor.submit(Arrays.asList(
                "vg.V(1).repeat(org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__().out()).times(2).iterate()"));

        wireMockRule.verify(13, postRequestedFor(urlPathEqualTo("/workspace1")));
    }

    @Test
    public void shouldClearGraph() throws RemoteException {
        stubFor(post(urlPathEqualTo("/workspace1"))
                .withQueryParam("format", equalTo("JSON"))
                .withQueryParam("operation", equalTo("updateGraph"))
                .withRequestBody(equalToJson("{\"dn\":{\"filter\":\"ALL\"}}"))
                .willReturn(aResponse()
                        .withStatus(200)));

        acceptor.submit(Arrays.asList("clear"));

        wireMockRule.verify(1, postRequestedFor(urlPathEqualTo("/workspace1")));
    }

    private static int pickOpenPort() {
        try (final ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
