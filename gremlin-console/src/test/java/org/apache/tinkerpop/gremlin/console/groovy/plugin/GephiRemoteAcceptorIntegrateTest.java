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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.io.input.NullInputStream;
import org.apache.tinkerpop.gremlin.console.GremlinGroovysh;
import org.apache.tinkerpop.gremlin.console.plugin.GephiRemoteAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteException;
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

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GephiRemoteAcceptorIntegrateTest {
    private static final Groovysh groovysh = new GremlinGroovysh();
    private static int port = pickOpenPort();

    private GephiRemoteAcceptor acceptor;

    private final InputStream inputStream  = new NullInputStream(0);
    private final OutputStream outputStream = new ByteArrayOutputStream();
    private final OutputStream errorStream = new ByteArrayOutputStream();
    private final IO io = new IO(inputStream, outputStream, errorStream);

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(port);

    @Before
    public void before() throws Exception {
        acceptor = new GephiRemoteAcceptor(groovysh, io);
        acceptor.configure(Arrays.asList("port", String.valueOf(port)));
    }

    @Test
    public void shouldConnectWithDefaults() throws RemoteException {
        assertThat(acceptor.connect(Collections.emptyList()).toString(), startsWith("Connection to Gephi - http://localhost:" + port + "/workspace0"));
    }

    @Test
    public void shouldSubmitGraph() throws RemoteException {
        stubFor(post(urlPathEqualTo("/workspace0"))
                .withQueryParam("format", equalTo("JSON"))
                .withQueryParam("operation", equalTo("updateGraph"))
                .willReturn(aResponse()
                        .withStatus(200)));

        stubFor(get(urlPathEqualTo("/workspace0"))
                .withQueryParam("operation", equalTo("getNode"))
                .willReturn(aResponse()
                        .withStatus(200)));

        acceptor.submit(Arrays.asList("g = org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph.open();g.addVertex();g"));

        wireMockRule.verify(2, postRequestedFor(urlPathEqualTo("/workspace0")));
        wireMockRule.verify(1, getRequestedFor(urlPathEqualTo("/workspace0")));
    }

    @Test
    public void shouldSubmitTraversal() throws RemoteException {
        stubFor(post(urlPathEqualTo("/workspace0"))
                .withQueryParam("format", equalTo("JSON"))
                .withQueryParam("operation", equalTo("updateGraph"))
                .willReturn(aResponse()
                        .withStatus(200)));

        acceptor.submit(Arrays.asList("g = org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.createModern().traversal();" +
                "traversal = g.V(2).store('1').in('knows').store('2').out('knows').has('age',org.apache.tinkerpop.gremlin.process.traversal.P.gt(30)).store('3').outE('created').has('weight',org.apache.tinkerpop.gremlin.process.traversal.P.gt(0.5d)).inV().store('4');" +
                "traversal.iterate();" +
                "traversal"));

        wireMockRule.verify(10, postRequestedFor(urlPathEqualTo("/workspace0")));
    }

    private static int pickOpenPort() {
        try (final ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
