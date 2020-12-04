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
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification.VertexProgramRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer.REJECT_BYTECODE;
import static org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer.REJECT_LAMBDA;
import static org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer.REJECT_MUTATE;
import static org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer.REJECT_OLAP;
import static org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer.REJECT_SUBGRAPH;
import static org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer.REJECT_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @author Marc de Lignie
 *
 * Run with:
 * mvn test --projects gremlin-server -Dtest=AuthorizerTest
 */
public class AuthorizerTest {

    final String BYTECODE = "bytecode";
    final String BYTECODE_LAMBDA = "bytecode-lambda";
    final String BYTECODE_MUTATE = "bytecode-mutate";
    final String BYTECODE_OLAP = "bytecode-OLAP";
    final String BYTECODE_SUBGRAPH = "bytecode-subgraph";

    AllowListAuthorizer authorizer;
    SubgraphTraversals subgraphTraversals;

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
        positiveBytecode("userclassic", "gclassic", BYTECODE);
        positiveBytecode("usermodern", "gmodern", BYTECODE);
        positiveBytecode("stephen", "gmodern", BYTECODE);
        positiveBytecode("userclassic", "gcrew", BYTECODE);
        positiveBytecode("usermodern", "gcrew", BYTECODE);
        positiveBytecode("stephen", "gcrew", BYTECODE);
        positiveBytecode("userclassic", "ggrateful", BYTECODE);
        positiveBytecode("usersink", "ggrateful", BYTECODE);
        positiveBytecode("anyuser", "ggrateful", BYTECODE);
        positiveBytecode("usersandbox", "gclassic", BYTECODE);
        positiveBytecode("marko", "gcrew", BYTECODE);
    }

    @Test
    public void shouldNotAuthorizeBytecodeRequest() {
        negativeBytecode("usersink", "gclassic", BYTECODE);
        negativeBytecode("usersink", "gmodern", BYTECODE);
        negativeBytecode("usersink", "gcrew", BYTECODE);
        negativeBytecode("anyuser", "gclassic", BYTECODE);
        negativeBytecode("anyuser", "gmodern", BYTECODE);
        negativeBytecode("anyuser", "gcrew", BYTECODE);
    }

    @Test
    public void shouldAuthorizeLambdaBytecodeRequest() throws AuthorizationException {
        positiveBytecode("usersandbox", "gclassic", BYTECODE_LAMBDA);
        positiveBytecode("marko", "gcrew", BYTECODE_LAMBDA);
    }

    @Test
    public void shouldNotAuthorizeLambdaBytecodeRequest() {
        negativeBytecode("userclassic", "gclassic", BYTECODE_LAMBDA);
        negativeBytecode("usermodern", "gmodern", BYTECODE_LAMBDA);
        negativeBytecode("stephen", "gmodern", BYTECODE_LAMBDA);
        negativeBytecode("userclassic", "gcrew", BYTECODE_LAMBDA);
        negativeBytecode("usermodern", "gcrew", BYTECODE_LAMBDA);
        negativeBytecode("stephen", "gcrew", BYTECODE_LAMBDA);
        negativeBytecode("userclassic", "ggrateful", BYTECODE_LAMBDA);
        negativeBytecode("usersink", "ggrateful", BYTECODE_LAMBDA);
        negativeBytecode("anyuser", "ggrateful", BYTECODE_LAMBDA);
    }

    @Test
    public void shouldAuthorizeMutatingBytecodeRequest() throws AuthorizationException {
        positiveBytecode("usersandbox", "gclassic", BYTECODE_MUTATE);
        positiveBytecode("marko", "gcrew", BYTECODE_MUTATE);
    }

    @Test
    public void shouldNotAuthorizeMutatingBytecodeRequest() {
        negativeBytecode("userclassic", "gclassic", BYTECODE_MUTATE);
        negativeBytecode("usermodern", "gmodern", BYTECODE_MUTATE);
        negativeBytecode("stephen", "gmodern", BYTECODE_MUTATE);
        negativeBytecode("userclassic", "gcrew", BYTECODE_MUTATE);
        negativeBytecode("usermodern", "gcrew", BYTECODE_MUTATE);
        negativeBytecode("stephen", "gcrew", BYTECODE_MUTATE);
        negativeBytecode("userclassic", "ggrateful", BYTECODE_MUTATE);
        negativeBytecode("usersink", "ggrateful", BYTECODE_MUTATE);
        negativeBytecode("anyuser", "ggrateful", BYTECODE_MUTATE);
    }

    @Test
    public void shouldAuthorizeOLAPBytecodeRequest() throws AuthorizationException {
        positiveBytecode("usersandbox", "gclassic", BYTECODE_OLAP);
        positiveBytecode("marko", "gcrew", BYTECODE_OLAP);
    }

    @Test
    public void shouldNotAuthorizeOLAPBytecodeRequest() {
        negativeBytecode("userclassic", "gclassic", BYTECODE_OLAP);
        negativeBytecode("usermodern", "gmodern", BYTECODE_OLAP);
        negativeBytecode("stephen", "gmodern", BYTECODE_OLAP);
        negativeBytecode("userclassic", "gcrew", BYTECODE_OLAP);
        negativeBytecode("usermodern", "gcrew", BYTECODE_OLAP);
        negativeBytecode("stephen", "gcrew", BYTECODE_OLAP);
        negativeBytecode("userclassic", "ggrateful", BYTECODE_OLAP);
        negativeBytecode("usersink", "ggrateful", BYTECODE_OLAP);
        negativeBytecode("anyuser", "ggrateful", BYTECODE_OLAP);
    }

    @Test
    public void shouldAuthorizeSubgraphStrategyBytecodeRequest() throws AuthorizationException {
        subgraphTraversals = new SubgraphTraversals();
        positiveBytecode("usersandbox", "gclassic", BYTECODE_SUBGRAPH);
        positiveBytecode("marko", "gcrew", BYTECODE_SUBGRAPH);
    }

    @Test
    public void shouldNotAuthorizeSubgraphStrategyBytecodeRequest() {
        subgraphTraversals = new SubgraphTraversals();
        negativeBytecode("userclassic", "gclassic", BYTECODE_SUBGRAPH);
        negativeBytecode("usermodern", "gmodern", BYTECODE_SUBGRAPH);
        negativeBytecode("stephen", "gmodern", BYTECODE_SUBGRAPH);
        negativeBytecode("userclassic", "gcrew", BYTECODE_SUBGRAPH);
        negativeBytecode("usermodern", "gcrew", BYTECODE_SUBGRAPH);
        negativeBytecode("stephen", "gcrew", BYTECODE_SUBGRAPH);
        negativeBytecode("userclassic", "ggrateful", BYTECODE_SUBGRAPH);
        negativeBytecode("usersink", "ggrateful", BYTECODE_SUBGRAPH);
        negativeBytecode("anyuser", "ggrateful", BYTECODE_SUBGRAPH);
    }

    @Test
    public void shouldAuthorizeStringRequest() throws AuthorizationException {
        authorizer.authorize(new AuthenticatedUser("usersandbox"), buildRequestMessage("gclassic"));
        authorizer.authorize(new AuthenticatedUser("marko"), buildRequestMessage("gcrew"));
    }

    @Test
    public void shouldNotAuthorizeStringReques() {
        negativeString("userclassic", "gclassic");
        negativeString("stephen", "gmodern");
        negativeString("userclassic", "gmodern");
        negativeString("usersink", "gclassic");
        negativeString("anyuser", "ggrateful");
    }

    private void positiveBytecode(final String username, final String traversalSource, final String requestType) throws AuthorizationException {
        final Map<String, String> aliases = new HashMap<>();
        aliases.put("g", traversalSource);
        authorizer.authorize(new AuthenticatedUser(username), bytecodeRequest(requestType), aliases);
    }

    private void negativeBytecode(final String username, final String traversalSource, final String requestType) {
        final Map<String, String> aliases = new HashMap<>();
        aliases.put("g", traversalSource);
        final String message;
        switch (requestType) {
            case BYTECODE:           message = String.format(REJECT_BYTECODE + ".", "[" + traversalSource + "]"); break;
            case BYTECODE_LAMBDA:    message = String.format(REJECT_BYTECODE + " using " + REJECT_LAMBDA + ".", "[" + traversalSource + "]"); break;
            case BYTECODE_MUTATE:    message = String.format(REJECT_BYTECODE + " using " + REJECT_MUTATE + ".", "[" + traversalSource + "]"); break;
            case BYTECODE_OLAP:      message = String.format(REJECT_BYTECODE + " using " + REJECT_OLAP + ".", "[" + traversalSource + "]"); break;
            case BYTECODE_SUBGRAPH:  message = String.format(REJECT_BYTECODE + " using " + REJECT_SUBGRAPH + ".", "[" + traversalSource + "]"); break;
            default: throw new IllegalArgumentException();
        }
        try {
            authorizer.authorize(new AuthenticatedUser(username), bytecodeRequest(requestType), aliases);
            fail("Test code did not fail while it should have failed!");
        } catch(AuthorizationException e) {
            assertEquals(message, e.getMessage());
        }
    }

    private Bytecode bytecodeRequest(final String requestType) {
        final GraphTraversalSource g = TinkerGraph.open().traversal();
        final Bytecode bytecode;

        switch (requestType) {
            case BYTECODE:           bytecode = g.V().asAdmin().getBytecode(); break;
            case BYTECODE_LAMBDA:    bytecode = g.V().map(Lambda.function("it.get()")).asAdmin().getBytecode(); break;
            case BYTECODE_MUTATE:    bytecode = g.withoutStrategies(ReadOnlyStrategy.class).V().addV().asAdmin().getBytecode(); break;
            case BYTECODE_OLAP:      bytecode = g.withoutStrategies(VertexProgramRestrictionStrategy.class).withComputer().V().asAdmin().getBytecode(); break;
            case BYTECODE_SUBGRAPH:  bytecode = subgraphTraversals.get(); break;
            default: throw new IllegalArgumentException();
        }
        return bytecode;
    }

    private void negativeString(final String username, final String traversalSource) {
        try {
            authorizer.authorize(new AuthenticatedUser(username), buildRequestMessage(traversalSource));
            fail("Test code did not fail while it should have failed!");
        } catch(AuthorizationException e) {
            assertEquals(REJECT_STRING, e.getMessage());
        }
    }

    private RequestMessage buildRequestMessage(final String traversalSource) {
        final String script = String.format("1+1; %s.V().map{it.get()}", traversalSource);
        return RequestMessage.build(Tokens.OPS_EVAL).addArg(Tokens.ARGS_GREMLIN, script).create();
    }

    private static class SubgraphTraversals implements Supplier<Bytecode> {
        final GraphTraversalSource g = TinkerGraph.open().traversal();
        final Iterator<Bytecode> mutatingBytecodes = Arrays.asList(
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getBytecode(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getBytecode(),
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getBytecode(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getBytecode(),
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getBytecode(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getBytecode(),
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getBytecode(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getBytecode(),
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getBytecode(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getBytecode(),
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getBytecode(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getBytecode()
        ).iterator();

        public Bytecode get() {
            return mutatingBytecodes.next();
        }
    }
}
