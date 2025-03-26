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

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class VertexProgramStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;


    @Test
    public void doTest() {
        final String repr = original.getGremlinLang().getGremlin();
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(new VertexProgramStrategy(Computer.compute()), ComputerVerificationStrategy.instance());
        original.asAdmin().setStrategies(strategies);
        original.asAdmin().applyStrategies();
        assertEquals(repr, optimized, original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        final ComputerResultStep computerResultStep = new ComputerResultStep(EmptyTraversal.instance());
        return Arrays.asList(new Traversal[][]{
                {__.V().out().count(), start().addStep(traversal(__.V().out().count())).addStep(computerResultStep)},
                {__.V().pageRank().out().count(), start().pageRank().asAdmin().addStep(traversal(__.V().out().count())).addStep(computerResultStep)},
                {__.V().out().pageRank(), start().addStep(traversal(__.V().out())).pageRank().asAdmin().addStep(traversal(__.identity())).addStep(computerResultStep)},
                {__.V().out().pageRank().count(), start().addStep(traversal(__.V().out())).pageRank().asAdmin().addStep(traversal(__.count())).addStep(computerResultStep)},
                {__.V().pageRank().order().limit(1), start().pageRank().asAdmin().addStep(traversal(__.V().order().limit(1))).addStep(computerResultStep)}
        });
    }

    private static GraphTraversal.Admin<?, ?> start() {
        return new DefaultGraphTraversal<>().asAdmin();
    }

    private static Step<?, ?> traversal(final Traversal<?, ?> traversal) {
        return new TraversalVertexProgramStep(EmptyTraversal.instance(), traversal.asAdmin());
    }
}
