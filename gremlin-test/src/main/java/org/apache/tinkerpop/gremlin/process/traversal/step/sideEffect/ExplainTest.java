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

package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.javatuples.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ExplainTest extends AbstractGremlinProcessTest {

    public abstract TraversalExplanation get_g_V_outE_identity_inV_explain();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_identity_inV_explain() {
        final TraversalExplanation explanation = get_g_V_outE_identity_inV_explain();
        if (explanation.getStrategyTraversals().stream().map(Pair::getValue0).filter(s -> s instanceof IdentityRemovalStrategy || s instanceof IncidentToAdjacentStrategy).count() == 2) {
            printTraversalForm(explanation.getOriginalTraversal());
            boolean beforeIncident = true;
            boolean beforeIdentity = true;
            for (final Pair<TraversalStrategy, Traversal.Admin<?, ?>> pair : explanation.getStrategyTraversals()) {
                if (pair.getValue0().getClass().equals(IncidentToAdjacentStrategy.class))
                    beforeIncident = false;
                if (pair.getValue0().getClass().equals(IdentityRemovalStrategy.class))
                    beforeIdentity = false;

                if (beforeIdentity)
                    assertEquals(1, TraversalHelper.getStepsOfClass(IdentityStep.class, pair.getValue1()).size());

                if (beforeIncident)
                    assertEquals(1, TraversalHelper.getStepsOfClass(EdgeVertexStep.class, pair.getValue1()).size());

                if (!beforeIdentity)
                    assertEquals(0, TraversalHelper.getStepsOfClass(IdentityStep.class, pair.getValue1()).size());

                if (!beforeIncident)
                    assertEquals(0, TraversalHelper.getStepsOfClass(EdgeVertexStep.class, pair.getValue1()).size());
            }
            assertFalse(beforeIncident);
        }
    }

    public static class Traversals extends ExplainTest {

        public TraversalExplanation get_g_V_outE_identity_inV_explain() {
            return g.V().outE().identity().inV().explain();
        }
    }

}
