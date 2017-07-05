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

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StepPositionTest {

    @Test
    public void shouldCorrectlyIdentifyStepIds() {
        testEachTraversal(__.out().in().groupCount().unfold().select("a").asAdmin());
    }

    @Test
    public void shouldNotIdentitySideEffectAndScopingLabels() {
        testEachTraversal(__.out().in().groupCount("a").unfold().union(__.out(), __.both().select("c")).select("b").groupCount("c").by(__.values("name")).where("a", P.neq("b")).asAdmin());
    }

    private void testEachTraversal(final Traversal.Admin<?, ?> traversal) {
        for (final Step step : traversal.getSteps()) {
            assertTrue(StepPosition.isStepId(step.getId()));
            if (step instanceof SideEffectCapable) {
                assertFalse(StepPosition.isStepId(((SideEffectCapable) step).getSideEffectKey()));
            }
            if (step instanceof Scoping) {
                ((Scoping) step).getScopeKeys().forEach(key -> assertFalse(StepPosition.isStepId(key)));
            }
            if (step instanceof TraversalParent) {
                ((TraversalParent) step).getLocalChildren().forEach(this::testEachTraversal);
                ((TraversalParent) step).getGlobalChildren().forEach(this::testEachTraversal);
            }
        }
    }
}
