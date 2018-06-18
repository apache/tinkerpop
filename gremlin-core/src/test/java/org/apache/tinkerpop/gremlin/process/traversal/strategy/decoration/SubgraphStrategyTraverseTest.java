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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class SubgraphStrategyTraverseTest {
    private static Traversal traversalWithAddV;

    static {
        final Graph mockedGraph = mock(Graph.class);
        final DefaultGraphTraversal t = new DefaultGraphTraversal<>(mockedGraph);
        t.asAdmin().addStep(new GraphStep<>(t.asAdmin(), Vertex.class, true));
        traversalWithAddV = t.addV();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"bothV()", __.bothV(), 1},
                {"inV()", __.inV(), 1},
                {"outV()", __.outV(), 1},
                {"in()", __.in(), 2},
                {"in(args)", __.in("test"), 2},
                {"both()", __.both(), 2},
                {"both(args)", __.both("test"), 2},
                {"out()", __.out(), 2},
                {"out(args)", __.out("test"), 2},
                {"out().inE().otherV", __.out().inE().otherV(), 4},
                {"addV()", traversalWithAddV, 2},
              //  {"addInE()", __.addInE("test", "x"), 1},
              //  {"addOutE()", __.addOutE("test", "x"), 1},
              //  {"addInE()", __.addInE("test", "x", "other", "args"), 1},
              //  {"addOutE()", __.addOutE("test", "x", "other", "args"), 1},
              //  {"addE(OUT)", __.addE(Direction.OUT, "test", "x"), 1},
              //  {"addE(IN)", __.addE(Direction.IN, "test", "x"), 1},
                {"in().out()", __.in().out(), 4},
              //  {"in().out().addInE()", __.in().out().addInE("test", "x"), 5},
              //  {"in().out().addOutE()", __.in().out().addOutE("test", "x"), 5},
              //  {"in().out().addE(OUT)", __.in().out().addE(Direction.OUT, "test", "x"), 5},
              //  {"in().out().addE(IN)", __.in().out().addE(Direction.IN, "test", "x"), 5},
                {"out().out().out()", __.out().out().out(), 6},
                {"in().out().in()", __.in().out().in(), 6},
                {"inE().outV().inE().outV()", __.inE().outV().inE().outV(), 4}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Parameterized.Parameter(value = 2)
    public int expectedInsertedSteps;

    @Test
    public void shouldSubgraph() {
        final SubgraphStrategy strategy = SubgraphStrategy.build().edges(__.identity()).vertices(__.identity()).create();
        strategy.apply(traversal.asAdmin());

        final List<TraversalFilterStep> steps = TraversalHelper.getStepsOfClass(TraversalFilterStep.class, traversal.asAdmin());
        assertEquals(expectedInsertedSteps, steps.size());
    }
}
