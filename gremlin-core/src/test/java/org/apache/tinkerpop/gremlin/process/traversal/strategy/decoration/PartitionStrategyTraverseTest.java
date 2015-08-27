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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class PartitionStrategyTraverseTest {
    private static Traversal traversalWithAddV;

    static {
        final Graph mockedGraph = mock(Graph.class);
        final DefaultGraphTraversal t = new DefaultGraphTraversal<>(mockedGraph);
        t.asAdmin().addStep(new GraphStep<>(t.asAdmin(), Vertex.class));
        traversalWithAddV = t.addV();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"bothV()", __.bothV(), 1, false},
                {"inV()", __.inV(), 1, false},
                {"outV()", __.outV(), 1, false},
                {"in()", __.in(), 1, false},
                {"in(args)", __.in("test"), 1, false},
                {"both()", __.both(), 1, false},
                {"both(args)", __.both("test"), 1, false},
                {"out()", __.out(), 1, false},
                {"out(args)", __.out("test"), 1, false},
                {"out().inE().otherV", __.out().inE().otherV(), 3, false},
                {"addV()", traversalWithAddV, 1, true},
                {"addE(test).from(x)", __.addE("test").from("x"), 0, true},
                {"addE(test).to(x)", __.addE("test").to("x"), 0, true},
                {"addE(test).from(x).property(other,args)", __.addE("test").from("x").property("other", "args"), 0, true},
                {"addE(test).to(x).property(other,args)", __.addE("test").to("x").property("other", "args"), 0, true},
                {"in().out()", __.in().out(), 2, false},
                {"in().out().addE(test).from(x)", __.in().out().addE("test").from("x"), 2, true},
                {"in().out().addE(test).to(x)", __.in().out().addE("test").to("x"), 2, true},
                {"out().out().out()", __.out().out().out(), 3, false},
                {"in().out().in()", __.in().out().in(), 3, false},
                {"inE().outV().inE().outV()", __.inE().outV().inE().outV(), 4, false}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Parameterized.Parameter(value = 2)
    public int expectedInsertedSteps;

    @Parameterized.Parameter(value = 3)
    public boolean hasMutatingStep;

    @Test
    public void shouldIncludeAdditionalHasStepsAndAppendPartitionOnMutatingSteps() {
        final PartitionStrategy strategy = PartitionStrategy.build()
                .partitionKey("p").writePartition("a").addReadPartition("a").create();

        if (hasMutatingStep) {
            if (TraversalHelper.hasStepOfAssignableClass(AddEdgeStep.class, traversal.asAdmin())) {
                strategy.apply(traversal.asAdmin());
                final List<AddEdgeStep> addEdgeSteps = TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal.asAdmin());
                assertEquals(1, addEdgeSteps.size());
                addEdgeSteps.forEach(s -> {
                    assertEquals("test", s.getParameters().get(T.label, () -> Edge.DEFAULT_LABEL));
                    assertEquals("a", s.getParameters().get("p", null));
                });
            } else if (TraversalHelper.hasStepOfAssignableClass(AddVertexStep.class, traversal.asAdmin())) {
                strategy.apply(traversal.asAdmin());
                final List<AddVertexStep> addVertexSteps = TraversalHelper.getStepsOfAssignableClass(AddVertexStep.class, traversal.asAdmin());
                assertEquals(1, addVertexSteps.size());
                addVertexSteps.forEach(s -> assertEquals("a", s.getParameters().get("p", null)));
            } else if (TraversalHelper.hasStepOfAssignableClass(AddVertexStartStep.class, traversal.asAdmin())) {
                strategy.apply(traversal.asAdmin());
                final List<AddVertexStartStep> addVertexSteps = TraversalHelper.getStepsOfAssignableClass(AddVertexStartStep.class, traversal.asAdmin());
                assertEquals(1, addVertexSteps.size());
                addVertexSteps.forEach(s -> assertEquals("a", s.getParameters().get("p", null)));
            } else
                fail("This test should not be marked as having a mutating step or there is something else amiss.");
        } else {
            strategy.apply(traversal.asAdmin());
        }
        //System.out.println(name + "::::::" + traversal.toString());

        final List<HasStep> steps = TraversalHelper.getStepsOfClass(HasStep.class, traversal.asAdmin());
        assertEquals(expectedInsertedSteps, steps.size());

        final List<String> keySet = new ArrayList<>(strategy.getReadPartitions());
        steps.forEach(s -> {
            assertEquals(1, s.getHasContainers().size());
            final HasContainer hasContainer = (HasContainer) s.getHasContainers().get(0);
            assertEquals("p", hasContainer.getKey());
            assertEquals(keySet, hasContainer.getValue());
            assertEquals(Contains.within, hasContainer.getBiPredicate());
        });
    }
}
