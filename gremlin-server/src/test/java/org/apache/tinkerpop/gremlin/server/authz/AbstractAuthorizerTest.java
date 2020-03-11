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
package org.apache.tinkerpop.gremlin.server.authz;

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer.REJECT_BYTECODE;
import static org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer.REJECT_LAMBDA;
import static org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer.REJECT_OLAP;
import static org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer.REJECT_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @author Marc de Lignie
 *
 * Run with:
 * mvn test --projects gremlin-server -Dtest=AbstractAuthorizerTest
 */
public class AbstractAuthorizerTest {

    final String BYTECODE = "bytecode";
    final String BYTECODE_LAMBDA = "bytecode-lambda";
    final String BYTECODE_OLAP = "bytecode-OLAP";
    final String GROOVY_STRING = "groovy-string";

    public AllowListAuthorizer authorizer;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setup() {
        final Map<String, Object> config = new HashMap<>();
        final String yamlName = "org/apache/tinkerpop/gremlin/server/allow-list.yaml";
        String file = Objects.requireNonNull(getClass().getClassLoader().getResource(yamlName)).getFile();
        config.put(AllowListAuthorizer.KEY_AUTHORIZATION_ALLOWLIST, file);
        authorizer = new AllowListAuthorizer();
        authorizer.setup(config);
    }

    @Test
    public void shouldAuthorizeBytecodeRequest() throws AuthorizationException {
        positiveCase("userclassic", "gclassic", BYTECODE);
        positiveCase("usermodern", "gmodern", BYTECODE);
        positiveCase("stephen", "gmodern", BYTECODE);
        positiveCase("userclassic", "gcrew", BYTECODE);
        positiveCase("usermodern", "gcrew", BYTECODE);
        positiveCase("stephen", "gcrew", BYTECODE);
        positiveCase("userclassic", "ggrateful", BYTECODE);
        positiveCase("usersink", "ggrateful", BYTECODE);
        positiveCase("anyuser", "ggrateful", BYTECODE);
        positiveCase("usersandbox", "gclassic", BYTECODE);
        positiveCase("marko", "gcrew", BYTECODE);
    }

    @Test
    public void shouldNotAuthorizeBytecodeRequest() {
        negativeCase("usersink", "gclassic", BYTECODE, String.format(REJECT_BYTECODE, "[gclassic]"));
        negativeCase("usersink", "gmodern", BYTECODE, String.format(REJECT_BYTECODE, "[gmodern]"));
        negativeCase("usersink", "gcrew", BYTECODE, String.format(REJECT_BYTECODE, "[gcrew]"));
        negativeCase("anyuser", "gclassic", BYTECODE, String.format(REJECT_BYTECODE, "[gclassic]"));
        negativeCase("anyuser", "gmodern", BYTECODE, String.format(REJECT_BYTECODE, "[gmodern]"));
        negativeCase("anyuser", "gcrew", BYTECODE, String.format(REJECT_BYTECODE, "[gcrew]"));
    }

    @Test
    public void shouldAuthorizeBytecodeLambdaRequest() throws AuthorizationException {
        positiveCase("usersandbox", "gclassic", BYTECODE_LAMBDA);
        positiveCase("marko", "gcrew", BYTECODE_LAMBDA);
    }

    @Test
    public void shouldNotAuthorizeBytecodeLambdaRequest() {
        negativeCase("userclassic", "gclassic", BYTECODE_LAMBDA, String.format(REJECT_LAMBDA, "[gclassic]"));
        negativeCase("usermodern", "gmodern", BYTECODE_LAMBDA, String.format(REJECT_LAMBDA, "[gmodern]"));
        negativeCase("stephen", "gmodern", BYTECODE_LAMBDA, String.format(REJECT_LAMBDA, "[gmodern]"));
        negativeCase("userclassic", "gcrew", BYTECODE_LAMBDA, String.format(REJECT_LAMBDA, "[gcrew]"));
        negativeCase("usermodern", "gcrew", BYTECODE_LAMBDA, String.format(REJECT_LAMBDA, "[gcrew]"));
        negativeCase("stephen", "gcrew", BYTECODE_LAMBDA, String.format(REJECT_LAMBDA, "[gcrew]"));
        negativeCase("userclassic", "ggrateful", BYTECODE_LAMBDA, String.format(REJECT_LAMBDA, "[ggrateful]"));
        negativeCase("usersink", "ggrateful", BYTECODE_LAMBDA, String.format(REJECT_LAMBDA, "[ggrateful]"));
        negativeCase("anyuser", "ggrateful", BYTECODE_LAMBDA, String.format(REJECT_LAMBDA, "[ggrateful]"));
    }

    @Test
    public void shouldAuthorizeBytecodeOLAPRequest() throws AuthorizationException {
        positiveCase("usersandbox", "gclassic", BYTECODE_OLAP);
        positiveCase("marko", "gcrew", BYTECODE_OLAP);
    }

    @Test
    public void shouldNotAuthorizeBytecodeOLAPRequest() {
        negativeCase("userclassic", "gclassic", BYTECODE_OLAP, String.format(REJECT_OLAP, "[gclassic]"));
        negativeCase("usermodern", "gmodern", BYTECODE_OLAP, String.format(REJECT_OLAP, "[gmodern]"));
        negativeCase("stephen", "gmodern", BYTECODE_OLAP, String.format(REJECT_OLAP, "[gmodern]"));
        negativeCase("userclassic", "gcrew", BYTECODE_OLAP, String.format(REJECT_OLAP, "[gcrew]"));
        negativeCase("usermodern", "gcrew", BYTECODE_OLAP, String.format(REJECT_OLAP, "[gcrew]"));
        negativeCase("stephen", "gcrew", BYTECODE_OLAP, String.format(REJECT_OLAP, "[gcrew]"));
        negativeCase("userclassic", "ggrateful", BYTECODE_OLAP, String.format(REJECT_OLAP, "[ggrateful]"));
        negativeCase("usersink", "ggrateful", BYTECODE_OLAP, String.format(REJECT_OLAP, "[ggrateful]"));
        negativeCase("anyuser", "ggrateful", BYTECODE_OLAP, String.format(REJECT_OLAP, "[ggrateful]"));
    }

    @Test
    public void shouldAuthorizeStringRequest() throws AuthorizationException {
        positiveCase("usersandbox", "gclassic", GROOVY_STRING);
        positiveCase("marko", "gcrew", GROOVY_STRING);
    }

    @Test
    public void shouldNotAuthorizeStringReques() {
        negativeCase("userclassic", "gclassic", GROOVY_STRING, REJECT_STRING);
        negativeCase("stephen", "gmodern", GROOVY_STRING, REJECT_STRING);
        negativeCase("userclassic", "gmodern", GROOVY_STRING, REJECT_STRING);
        negativeCase("usersink", "gclassic", GROOVY_STRING, REJECT_STRING);
        negativeCase("anyuser", "ggrateful", GROOVY_STRING, REJECT_STRING);
    }

    private void positiveCase(final String username, final String traversalSource, final String requestType) throws AuthorizationException {
        final RequestMessage requestMessage = buildRequestMessage(traversalSource, requestType);
        authorizer.authorize(new AuthenticatedUser(username), requestMessage);
    }

    private void negativeCase(final String username, final String traversalSource, final String requestType, final String message) {
        final RequestMessage requestMessage = buildRequestMessage(traversalSource, requestType);
        try {
            authorizer.authorize(new AuthenticatedUser(username), requestMessage);
            fail("Test code did not fail while it should have failed!");
        } catch(AuthorizationException e) {
            assertEquals(message, e.getMessage());
        }
    }

    private RequestMessage buildRequestMessage(final String traversalSource, final String requestType) {
        final GraphTraversalSource g = TinkerGraph.open().traversal();
        final Bytecode bytecode;

        switch (requestType) {
            case BYTECODE:        bytecode = g.V().asAdmin().getBytecode(); break;
            case BYTECODE_LAMBDA: bytecode = g.V().map(Lambda.function("it.get()")).asAdmin().getBytecode(); break;
            case BYTECODE_OLAP: bytecode = g.withComputer().V().asAdmin().getBytecode(); break;
            case GROOVY_STRING:   {
                final String scriptx = String.format("1+1; %s.V().map{it.get()}", traversalSource);
                return RequestMessage.build(Tokens.OPS_EVAL).addArg(Tokens.ARGS_GREMLIN, scriptx).create();
            }
            default: throw new IllegalArgumentException();
        }
        final Map<String, String> aliases = new HashMap<>();
        aliases.put("g", traversalSource);
        return RequestMessage.build(Tokens.OPS_BYTECODE).addArg(Tokens.ARGS_GREMLIN, bytecode).addArg(Tokens.ARGS_ALIASES, aliases).create();
    }
}
