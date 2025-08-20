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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeStepPlaceholder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class RangeLocalStepTest extends GValueStepTest {

    private static final String LOW_NAME = "low";
    private static final String HIGH_NAME = "high";
    private static final long LOW_VALUE = 1L;
    private static final long HIGH_VALUE = 10L;

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.limit(Scope.local, HIGH_VALUE),
                __.skip(Scope.local, 9L), // TODO:: best to edit this to __.skip(Scope.local, 10L) following resolution of TINKERPOP-3170
                __.range(Scope.local, LOW_VALUE, HIGH_VALUE),
                __.limit(Scope.local, GValue.of("limit", HIGH_VALUE)),
                __.skip(Scope.local, GValue.of("skip", 9L)),
                __.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.limit(Scope.local, GValue.of("limit", HIGH_VALUE)), Set.of("limit")),
                Pair.of(__.skip(Scope.local, GValue.of("skip", HIGH_VALUE)), Set.of("skip")),
                Pair.of(__.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)), Set.of(LOW_NAME, HIGH_NAME))
        );
    }

    @Test
    public void getLowHighRangeNonGValue() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(Scope.local, LOW_VALUE, HIGH_VALUE).asAdmin();
        assertEquals((Long) LOW_VALUE, ((RangeLocalStep) traversal.getSteps().get(0)).getLowRange());
        assertEquals((Long) HIGH_VALUE, ((RangeLocalStep) traversal.getSteps().get(0)).getHighRange());
        verifyNoVariables(traversal);
    }

    @Test
    public void getLowHighRangeGValueSafeShouldNotPinVariables() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals(LOW_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).getLowRangeGValueSafe());
        assertEquals(HIGH_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).getHighRangeGValueSafe());
        verifyVariables(traversal, Set.of(), Set.of(LOW_NAME, HIGH_NAME));
    }

    @Test
    public void getLowShouldPinVariable() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals((Long) LOW_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).getLowRange());
        verifyVariables(traversal, Set.of(LOW_NAME), Set.of(HIGH_NAME));
    }

    @Test
    public void getHighShouldPinVariable() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals((Long) HIGH_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).getHighRange());
        verifyVariables(traversal, Set.of(HIGH_NAME), Set.of(LOW_NAME));
    }

    @Test
    public void getLowHighRangeGValueFromConcreteStep() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals((Long) LOW_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getLowRange());
        assertEquals((Long) HIGH_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getHighRange());
    }
}
