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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.util.TestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.repeat;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.sum;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class StandardVerificationStrategyTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() throws Exception {

        TestSupport.generateTempFile(StandardVerificationStrategyTest.class, "shouldBeVerified", ".kryo");

        return Arrays.asList(new Object[][]{
                // traversals that should fail verification
                {repeat(out().fold().unfold()).times(2), false},
                {repeat(sum()).times(2), false},
                {repeat(out().count()), false},
                {__.V().profile(), true},
                {__.V().profile("metrics").cap("metrics"), true}
        });
    }

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin traversal;

    @Parameterized.Parameter(value = 1)
    public Boolean legalTraversal;

    @Test
    public void shouldBeVerified() {
        final String repr = traversal.getGremlinLang().getGremlin();
        final Traversal copy = copyAndConfigureTraversal(traversal);

        if (legalTraversal) {
            copy.asAdmin().applyStrategies();

            // try to also apply strategies with iterate() so that a DiscardStep is added - for consistency sake we want
            // to be able to run a profile and get no result back with this.
            final Traversal forIteration = copyAndConfigureTraversal(traversal);
            forIteration.iterate();
        } else {
            try {
                copy.asAdmin().applyStrategies();
                fail("The strategy should not allow traversal: " + repr);
            } catch (Exception ise) {
                assertThat(ise, instanceOf(VerificationException.class));
            }
        }
    }

    private static Traversal copyAndConfigureTraversal(final Traversal traversalToCopy) {
        final Traversal copy = traversalToCopy.asAdmin().clone();
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(StandardVerificationStrategy.instance());

        // just add a junk requirement
        RequirementsStrategy.addRequirements(strategies, TraverserRequirement.BULK);

        copy.asAdmin().setStrategies(strategies);
        return copy;
    }
}
