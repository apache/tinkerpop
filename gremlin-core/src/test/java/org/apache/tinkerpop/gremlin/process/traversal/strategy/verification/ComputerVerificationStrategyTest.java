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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.max;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.min;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.sum;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class ComputerVerificationStrategyTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // illegal
                {__.where(__.out().values("name")), false},
                {__.local(out().out()), false},
                // legal
                {__.values("age").union(max(), min(), sum()), true},
                {__.count().sum(), true},
                {__.where("a", P.eq("b")).out(), true},
                {__.where(__.and(outE("knows"), outE("created"))).values("name"), true},

        });
    }

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin traversal;

    @Parameterized.Parameter(value = 1)
    public boolean legal;

    @Test
    public void shouldBeVerifiedIllegal() {
        final String repr = traversal.getGremlinLang().getGremlin();

        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(ComputerVerificationStrategy.instance());
        this.traversal.asAdmin().setParent(new TraversalVertexProgramStep(EmptyTraversal.instance(), EmptyTraversal.instance())); // trick it
        this.traversal.asAdmin().setStrategies(strategies);
        try {
            this.traversal.asAdmin().applyStrategies();
            if (!this.legal)
                fail("The traversal should not be allowed: " + repr);
        } catch (final VerificationException ise) {
            if (this.legal)
                fail("The traversal should be allowed: " + repr);
        }
    }

}
