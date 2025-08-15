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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class IsStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.count().is(0),
                __.count().is(P.gt(0)),
                __.count().is(1),
                __.count().is(GValue.of("x", 0)),
                __.count().is(P.gt(GValue.of("x", 0)))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.count().is(GValue.of("x", 0)), Set.of("x")),
                Pair.of(__.count().is(P.gt(GValue.of("x", 0))), Set.of("x"))
        );
    }

    @Test
    public void getPredicateGValueSafeShouldNotPinVariable() {
        final GraphTraversal.Admin<Object, Long> traversal = __.count().is(GValue.of("x", 0)).asAdmin();
        assertNotNull(((IsStepPlaceholder) traversal.getSteps().get(1)).getPredicateGValueSafe());
        GValueManager gValueManager = traversal.getGValueManager();
        assertTrue(gValueManager.hasUnpinnedVariables());
        assertEquals(1, gValueManager.getUnpinnedVariableNames().size());
        assertEquals("x", gValueManager.getUnpinnedVariableNames().iterator().next());
    }

    @Test
    public void getPredicateShouldPinVariable() {
        final GraphTraversal.Admin<Object, Long> traversal = __.count().is(P.gt(GValue.of("x", 0))).asAdmin();
        assertNotNull(((IsStepPlaceholder) traversal.getSteps().get(1)).getPredicate());
        GValueManager gValueManager = traversal.getGValueManager();
        assertFalse(gValueManager.hasUnpinnedVariables());
        assertEquals(1, gValueManager.getPinnedVariableNames().size());
        assertEquals("x", gValueManager.getPinnedVariableNames().iterator().next());
    }
}
