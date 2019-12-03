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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ScalarMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OptionsStrategyTest {

    @Test
    public void shouldAddOptionsToTraversal() {
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource optionedG = graph.traversal().withStrategies(OptionsStrategy.build().with("a", "test").with("b").create());
        assertOptions(optionedG);
    }

    @Test
    public void shouldAddOptionsToTraversalUsingWith() {
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource optionedG = graph.traversal().with("a", "test").with("b");
        assertOptions(optionedG);
    }

    private static void assertOptions(final GraphTraversalSource optionedG) {
        GraphTraversal t = optionedG.inject(1);
        t = t.asAdmin().addStep(new ScalarMapStep<Object, Object>(t.asAdmin()) {
            @Override
            protected Object map(final Traverser.Admin<Object> traverser) {
                final OptionsStrategy strategy = traversal.asAdmin().getStrategies().getStrategy(OptionsStrategy.class).get();
                return Arrays.asList(strategy.getOptions().get("a"), strategy.getOptions().get("b"));
            }
        });
        assertThat((Collection<Object>) t.next(), contains("test", true));
    }
}
