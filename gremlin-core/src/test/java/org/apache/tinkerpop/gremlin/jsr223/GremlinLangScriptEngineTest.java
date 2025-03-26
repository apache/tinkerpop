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
package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxyTest;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GremlinLangScriptEngineTest {

    private final static GremlinLangScriptEngine scriptEngine = new GremlinLangScriptEngine();
    private final static GraphTraversalSource g = EmptyGraph.instance().traversal();

    static {
        scriptEngine.put("g", g);
    }

    @Test
    public void shouldEvalGremlinScript() throws ScriptException {
        final Object result = scriptEngine.eval("g.V()");
        assertThat(result, instanceOf(Traversal.Admin.class));
        assertEquals(g.V().asAdmin().getGremlinLang(), ((Traversal.Admin) result).getGremlinLang());
    }

    @Test
    public void shouldEvalGremlinScriptWithParameters() throws ScriptException {
        final Bindings b = new SimpleBindings();
        b.put("g", g);
        b.put("x", 100);
        b.put("y", 1000);
        b.put("z", 10000);

        final Object result = scriptEngine.eval("g.V(x, y, z)", b);
        assertThat(result, instanceOf(Traversal.Admin.class));
        assertEquals(g.V(100, 1000, 10000).asAdmin().getGremlinLang(), ((Traversal.Admin) result).getGremlinLang());
    }

    public static class TestStrategy<S extends TraversalStrategy> extends AbstractTraversalStrategy<S> {
        private final Configuration configuration;

        private TestStrategy(final Configuration configuration) {
            this.configuration = configuration;
        }
        @Override
        public void apply(Traversal.Admin traversal) {
            // Do nothing
        }

        @Override
        public Configuration getConfiguration() {
            return configuration;
        }

        public static TestStrategy create(Configuration configuration) {
            return new TestStrategy(configuration);
        }
    }

    @Test
    public void shouldReconstructCustomRegisteredStrategy() throws ScriptException {
        TraversalStrategies.GlobalCache.registerStrategy(TestStrategy.class);

        GraphTraversal traversal = (GraphTraversal) scriptEngine.eval("g.withStrategies(new TestStrategy(stringKey:\"stringValue\",intKey:1,booleanKey:true)).V()");

        TestStrategy reconstructedStrategy = traversal.asAdmin().getStrategies().getStrategy(TestStrategy.class).get();

        assertNotNull(reconstructedStrategy);

        MapConfiguration expectedConfig = new MapConfiguration(new HashMap<String, Object>() {{
            put("stringKey", "stringValue");
            put("intKey", 1);
            put("booleanKey", true);
        }});

        Set<String> expectedKeys = new HashSet<>();
        Set<String> actualKeys = new HashSet<>();
        expectedConfig.getKeys().forEachRemaining((key) -> expectedKeys.add(key));
        reconstructedStrategy.getConfiguration().getKeys().forEachRemaining((key) -> actualKeys.add(key));

        assertEquals(expectedKeys, actualKeys);

        expectedKeys.forEach((key) -> {
            assertEquals(expectedConfig.get(Object.class, key), reconstructedStrategy.getConfiguration().get(Object.class, key));
        });
    }
}
