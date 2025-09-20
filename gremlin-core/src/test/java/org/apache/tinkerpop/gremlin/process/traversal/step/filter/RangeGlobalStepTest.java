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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class RangeGlobalStepTest extends GValueStepTest {

    private static final String LOW_NAME = "low";
    private static final String HIGH_NAME = "high";
    private static final long LOW_VALUE = 2L;
    private static final long HIGH_VALUE = 20L;
    
    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.limit(10L),
                __.skip(10L),
                __.range(1L, 10L),
                __.limit(GValue.of("limit", 10L)),
                __.skip(GValue.of("skip", 10L)),
                __.range(GValue.of(LOW_NAME, 1L), GValue.of(HIGH_NAME, 10L))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
            Pair.of(__.limit(GValue.of("limit", 10L)), Set.of("limit")),
            Pair.of(__.skip(GValue.of("skip", 10L)), Set.of("skip")),
            Pair.of(__.range(GValue.of(LOW_NAME, 1L), GValue.of(HIGH_NAME, 10L)), Set.of(LOW_NAME, HIGH_NAME))
        );
    }

    @Test
    public void getLowHighRangeNonGValue() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(LOW_VALUE, HIGH_VALUE).asAdmin();
        assertEquals((Long) LOW_VALUE, ((RangeGlobalStep) traversal.getSteps().get(0)).getLowRange());
        assertEquals((Long) HIGH_VALUE, ((RangeGlobalStep) traversal.getSteps().get(0)).getHighRange());
        verifyNoVariables(traversal);
    }

    @Test
    public void getLowHighRangeAsGValueShouldNotPinVariables() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals(GValue.of(LOW_NAME, LOW_VALUE), ((RangeGlobalStepPlaceholder) traversal.getSteps().get(0)).getLowRangeAsGValue());
        assertEquals(GValue.of(HIGH_NAME, HIGH_VALUE), ((RangeGlobalStepPlaceholder) traversal.getSteps().get(0)).getHighRangeAsGValue());
        verifyVariables(traversal, Set.of(), Set.of(LOW_NAME, HIGH_NAME));
    }

    @Test
    public void getLowShouldPinVariable() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals((Long) LOW_VALUE, ((RangeGlobalStepPlaceholder) traversal.getSteps().get(0)).getLowRange());
        verifyVariables(traversal, Set.of(LOW_NAME), Set.of(HIGH_NAME));
    }

    @Test
    public void getHighShouldPinVariable() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals((Long) HIGH_VALUE, ((RangeGlobalStepPlaceholder) traversal.getSteps().get(0)).getHighRange());
        verifyVariables(traversal, Set.of(HIGH_NAME), Set.of(LOW_NAME));
    }

    @Test
    public void getLowHighRangeGValueFromConcreteStep() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals((Long) LOW_VALUE, ((RangeGlobalStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getLowRange());
        assertEquals((Long) HIGH_VALUE, ((RangeGlobalStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getHighRange());
    }
}
