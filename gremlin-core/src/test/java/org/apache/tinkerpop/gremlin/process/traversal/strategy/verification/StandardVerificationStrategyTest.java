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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.RequirementsStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.repeat;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.sum;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class StandardVerificationStrategyTest {

    private static final RequirementsStep emptyRequirementStep = new RequirementsStep<>(null, Collections.EMPTY_SET);

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // traversals that should fail verification
                {"__.repeat(out().fold().unfold()).times(2)", repeat(out().fold().unfold()).times(2), false},
                {"__.repeat(sum()).times(2)", repeat(sum()).times(2), false},
                {"__.repeat(out().count())", repeat(out().count()), false},
                // traversals that should pass verification
                {"__.V().profile().requirementsStep()",
                        __.V().profile().asAdmin().addStep(emptyRequirementStep), true},
                {"__.V().profile('metrics').cap('metrics').requirementsStep()",
                        __.V().profile("metrics").asAdmin().addStep(emptyRequirementStep), true}
        });
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Parameterized.Parameter(value = 2)
    public Boolean legalTraversal;

    @Test
    public void shouldBeVerified() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(StandardVerificationStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);

        if (legalTraversal) {
            traversal.asAdmin().applyStrategies();
        } else {
            try {
                traversal.asAdmin().applyStrategies();
                fail("The strategy should not allow traversal: " + this.traversal);
            } catch (IllegalStateException ise) {
                assertTrue(true);
            }
        }
    }
}
