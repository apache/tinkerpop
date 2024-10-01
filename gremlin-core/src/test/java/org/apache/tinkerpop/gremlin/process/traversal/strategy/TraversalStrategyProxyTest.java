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
package org.apache.tinkerpop.gremlin.process.traversal.strategy;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TraversalStrategyProxyTest {

    private static class TestStrategy<S extends TraversalStrategy> extends AbstractTraversalStrategy<S> {
        @Override
        public void apply(Traversal.Admin traversal) {
            // Do nothing
        }
    }

    @Test
    public void shouldAddCustomStrategyToGremlinScript() {
        TraversalStrategyProxy strategyProxy = new TraversalStrategyProxy("TestStrategy");
        GraphTraversalSource g = EmptyGraph.instance().traversal();
        GraphTraversal traversal = g.withStrategies(strategyProxy).V();
        assertEquals("g.withStrategies(TestStrategy).V()", traversal.asAdmin().getGremlinLang().getGremlin());
    }

    @Test
    public void shouldAddCustomStrategyWithConfigToGremlinScript() {
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("stringKey", "stringValue");
        configMap.put("intKey", 1);
        configMap.put("booleanKey", true);
        TraversalStrategyProxy strategyProxy = new TraversalStrategyProxy("TestStrategy", new MapConfiguration(configMap));

        GraphTraversalSource g = EmptyGraph.instance().traversal();
        GraphTraversal traversal = g.withStrategies(strategyProxy).V();

        assertEquals("g.withStrategies(new TestStrategy(stringKey:\"stringValue\",intKey:1,booleanKey:true)).V()", traversal.asAdmin().getGremlinLang().getGremlin());
    }
}
