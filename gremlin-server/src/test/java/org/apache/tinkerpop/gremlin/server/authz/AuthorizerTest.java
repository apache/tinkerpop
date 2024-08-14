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

import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
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

    AllowListAuthorizer authorizer;

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
        return RequestMessage.build(script).create();
    }

    private static class SubgraphTraversals implements Supplier<GremlinLang> {
        final GraphTraversalSource g = TinkerGraph.open().traversal();
        final Iterator<GremlinLang> mutatingBytecodes = Arrays.asList(
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getGremlinLang(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getGremlinLang(),
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getGremlinLang(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getGremlinLang(),
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getGremlinLang(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getGremlinLang(),
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getGremlinLang(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getGremlinLang(),
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getGremlinLang(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getGremlinLang(),
                g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getGremlinLang(),
                g.withStrategies(SubgraphStrategy.build().vertices(__.bothE()).create()).V().asAdmin().getGremlinLang()
        ).iterator();

        public GremlinLang get() {
            return mutatingBytecodes.next();
        }
    }
}
