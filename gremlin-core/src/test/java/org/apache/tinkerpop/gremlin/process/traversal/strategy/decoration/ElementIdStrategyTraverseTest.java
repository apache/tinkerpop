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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeByPathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class ElementIdStrategyTraverseTest {
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
                {"addV()", traversalWithAddV, 1},
                {"addInE()", __.addInE("test", "x"), 0},
                {"addOutE()", __.addOutE("test", "x"), 0},
                {"addInE()", __.addInE("test", "x", "other", "args"), 0},
                {"addOutE()", __.addOutE("test", "x", "other", "args"), 0},
                {"addE(OUT)", __.addE(Direction.OUT, "test", "x"), 0},
                {"addE(IN)", __.addE(Direction.IN, "test", "x"), 0},
                {"out().id()", __.out().id(), 1},
                {"in().id()", __.in().id(), 1},
                {"outE().id()", __.outE().id(), 1},
                {"inE().id()", __.inE().id(), 1},
                {"bothE().id()", __.bothE().id(), 1},
                {"bothE().otherV().id()", __.bothE().otherV().id(), 2},
                {"in().out().addInE()", __.in().out().addInE("test", "x"), 2},
                {"in().out().addOutE()", __.in().out().addOutE("test", "x"), 2},
                {"in().out().addE(OUT)", __.in().out().addE(Direction.OUT, "test", "x"), 2},
                {"in().out().addE(IN)", __.in().out().addE(Direction.IN, "test", "x"), 2}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Parameterized.Parameter(value = 2)
    public int expectedInsertedSteps;

    @Test
    public void shouldAlterTraversalToIncludeIdWhereNecessary() {
        final ElementIdStrategy strategy = ElementIdStrategy.build().create();
        strategy.apply(traversal.asAdmin());

        final Step s = (Step) traversal.asAdmin().getSteps().get(expectedInsertedSteps);
        if(s instanceof AddVertexStep)
            assertTrue(ElementHelper.getKeys(((AddVertexStep) s).getKeyValues()).contains(strategy.getIdPropertyKey()));
        else if(s instanceof AddVertexStartStep)
            assertTrue(ElementHelper.getKeys(((AddVertexStartStep) s).getKeyValues()).contains(strategy.getIdPropertyKey()));
        else if(s instanceof AddEdgeByPathStep)
            assertTrue(ElementHelper.getKeys(((AddEdgeByPathStep) s).getKeyValues()).contains(strategy.getIdPropertyKey()));
        else if(s instanceof AddEdgeStep)
            assertTrue(ElementHelper.getKeys(((AddEdgeStep) s).getKeyValues()).contains(strategy.getIdPropertyKey()));
        else if(s instanceof PropertiesStep)
            assertEquals(strategy.getIdPropertyKey(), ((PropertiesStep) s).getPropertyKeys()[0]);
        else
            fail("Check test definition - the expectedInsertedSteps should be the index of the step to trigger the ID substitution");
    }
}
