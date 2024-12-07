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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsIterableContaining.hasItem;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class PartitionStrategyTest {

    @RunWith(Parameterized.class)
    public static class PartitionStrategyTraverseTest {

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {create().bothV(), 1, false},
                    {create().inV(), 1, false},
                    {create().outV(), 1, false},
                    {create().in(), 1, false},
                    {create().in("test"), 1, false},
                    {create().both(), 1, false},
                    {create().both("test"), 1, false},
                    {create().out(), 1, false},
                    {create().out("test"), 1, false},
                    {create().out().inE().otherV(), 3, false},
                    {create(Vertex.class).addV(), 1, true},
                    {create().addE("test").from("x"), 0, true},
                    {create().addE("test").to("x"), 0, true},
                    {create().addE("test").from("x").property("other", "args"), 0, true},
                    {create().addE("test").to("x").property("other", "args"), 0, true},
                    {create().in().out(), 2, false},
                    {create().in().out().addE("test").from("x"), 2, true},
                    {create().in().out().addE("test").to("x"), 2, true},
                    {create().out().out().out(), 3, false},
                    {create().in().out().in(), 3, false},
                    {create().inE().outV().inE().outV(), 4, false},
                    {create().bothV().hasLabel("person"), 2, false},
                    {create().inV().hasLabel("person").has("name"), 2, false},  // just 2 coz has("name") is TraversalFilterStep and not a has() that goes in a container :/
                    {create().outV().hasLabel("person").out(), 3, false},
                    {create().outV().hasLabel("person").out().hasLabel("software"), 4, false},
                    {create().in().hasLabel("person").out().outE().hasLabel("knows").groupCount(), 5, false},
                    {create().out().inE().otherV().hasLabel("person"), 4, false}});
        }

        @Parameterized.Parameter(value = 0)
        public Traversal.Admin traversal;

        @Parameterized.Parameter(value = 1)
        public int expectedInsertedSteps;

        @Parameterized.Parameter(value = 2)
        public boolean hasMutatingStep;

        @Test
        public void shouldIncludeAdditionalHasStepsAndAppendPartitionOnMutatingSteps() {
            final String repr = traversal.getGremlinLang().getGremlin();
            final PartitionStrategy strategy = PartitionStrategy.build()
                    .partitionKey("p").writePartition("a").readPartitions("a").create();

            if (hasMutatingStep) {
                if (TraversalHelper.hasStepOfAssignableClass(AddEdgeStep.class, traversal.asAdmin())) {
                    strategy.apply(traversal.asAdmin());
                    final List<AddEdgeStep> addEdgeSteps = TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal.asAdmin());
                    assertEquals(1, addEdgeSteps.size());
                    addEdgeSteps.forEach(s -> {
                        assertEquals(repr, GValue.of(null, "test"), s.getParameters().get(T.label, () -> Edge.DEFAULT_LABEL).get(0));
                        assertEquals(repr, "a", s.getParameters().get("p", null).get(0));
                    });
                } else if (TraversalHelper.hasStepOfAssignableClass(AddVertexStep.class, traversal.asAdmin())) {
                    strategy.apply(traversal.asAdmin());
                    final List<AddVertexStep> addVertexSteps = TraversalHelper.getStepsOfAssignableClass(AddVertexStep.class, traversal.asAdmin());
                    assertEquals(repr, 1, addVertexSteps.size());
                    addVertexSteps.forEach(s -> assertEquals(repr, "a", s.getParameters().get("p", null).get(0)));
                } else if (TraversalHelper.hasStepOfAssignableClass(AddVertexStartStep.class, traversal.asAdmin())) {
                    strategy.apply(traversal.asAdmin());
                    final List<AddVertexStartStep> addVertexSteps = TraversalHelper.getStepsOfAssignableClass(AddVertexStartStep.class, traversal.asAdmin());
                    assertEquals(repr, 1, addVertexSteps.size());
                    addVertexSteps.forEach(s -> assertEquals(repr, "a", s.getParameters().get("p", null).get(0)));
                } else
                    fail("This test should not be marked as having a mutating step or there is something else amiss.");
            } else {
                strategy.apply(traversal.asAdmin());
            }

            final List<HasStep> steps = TraversalHelper.getStepsOfClass(HasStep.class, traversal.asAdmin());
            assertEquals(repr, expectedInsertedSteps, steps.size());

            final List<String> keySet = new ArrayList<>(strategy.getReadPartitions());
            steps.forEach(s -> {
                assertEquals(repr, 1, s.getHasContainers().size());
                final HasContainer hasContainer = (HasContainer) s.getHasContainers().get(0);

                // if it is a partition has() then ensure it is not followed by has(). if it is something else, let it
                // be followed by partition has()
                if (hasContainer.getKey().equals("p")) {
                    assertEquals(repr, keySet, hasContainer.getValue());
                    assertEquals(repr, Contains.within, hasContainer.getBiPredicate());

                    // no has() after the partition filter
                    assertThat(s.getNextStep(), not(instanceOf(HasStep.class)));
                } else {
                    // last has() in the sequence should be the partition has()
                    Step insertAfter = s;
                    while (insertAfter.getNextStep() instanceof HasStep) {
                        insertAfter = insertAfter.getNextStep();
                    }

                    final HasContainer insertAfterHasContainer = (HasContainer) ((HasStep) insertAfter).getHasContainers().get(0);

                    assertEquals(repr, "p", insertAfterHasContainer.getKey());
                    assertEquals(repr, keySet, insertAfterHasContainer.getValue());
                    assertEquals(repr, Contains.within, insertAfterHasContainer.getBiPredicate());
                }
            });
        }

        public static GraphTraversal create() {
            return create(null);
        }

        public static GraphTraversal create(final Class<? extends Element> clazz) {
            final Graph mockedGraph = mock(Graph.class);
            final Graph.Features features = mock(Graph.Features.class);
            final Graph.Features.VertexFeatures vertexFeatures = mock(Graph.Features.VertexFeatures.class);
            when(mockedGraph.features()).thenReturn(features);
            when(features.vertex()).thenReturn(vertexFeatures);
            when(vertexFeatures.getCardinality(any())).thenReturn(VertexProperty.Cardinality.single);
            final DefaultGraphTraversal t = new DefaultGraphTraversal<>(mockedGraph);
            if (clazz != null) t.asAdmin().addStep(new GraphStep<>(t.asAdmin(), clazz, true));
            return t;
        }
    }

    public static class RewriteTest {
        @Test(expected = IllegalStateException.class)
        public void shouldNotConstructWithoutPartitionKey() {
            PartitionStrategy.build().create();
        }

        @Test
        public void shouldConstructPartitionStrategy() {
            final PartitionStrategy strategy = PartitionStrategy.build()
                    .partitionKey("p").writePartition("a").readPartitions("a").create();
            assertEquals("a", strategy.getReadPartitions().iterator().next());
            assertEquals(1, strategy.getReadPartitions().size());
            assertEquals("p", strategy.getPartitionKey());
        }

        @Test
        public void shouldConstructPartitionStrategyWithMultipleReadPartitions() {
            final PartitionStrategy strategy = PartitionStrategy.build()
                    .partitionKey("p").writePartition("a")
                    .readPartitions("a")
                    .readPartitions("b")
                    .readPartitions("c").create();

            assertThat(strategy.getReadPartitions(), hasItem("a"));
            assertThat(strategy.getReadPartitions(), hasItem("b"));
            assertThat(strategy.getReadPartitions(), hasItem("c"));
            assertEquals(3, strategy.getReadPartitions().size());
            assertEquals("p", strategy.getPartitionKey());
        }
    }
}
