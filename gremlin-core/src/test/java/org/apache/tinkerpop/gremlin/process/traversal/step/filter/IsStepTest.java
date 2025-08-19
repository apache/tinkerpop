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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class IsStepTest extends GValueStepTest {

    private static final String GVALUE_NAME = "x";
    private static final int VALUE = 0;

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.count().is(VALUE),
                __.count().is(P.gt(VALUE)),
                __.count().is(1),
                __.count().is(GValue.of(GVALUE_NAME, VALUE)),
                __.count().is(P.gt(GValue.of(GVALUE_NAME, VALUE)))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.count().is(GValue.of(GVALUE_NAME, VALUE)), Set.of(GVALUE_NAME)),
                Pair.of(__.count().is(P.gt(GValue.of(GVALUE_NAME, VALUE))), Set.of(GVALUE_NAME))
        );
    }
    
    @Test
    public void getPredicateNonGValue() {
        final GraphTraversal.Admin<Object, Long> traversal = __.count().is(VALUE).asAdmin();
        assertNotNull(((IsStep) traversal.getSteps().get(1)).getPredicate());
        verifyNoVariables(traversal);
    }

    @Test
    public void getPredicateGValueSafeShouldNotPinVariable() {
        final GraphTraversal.Admin<Object, Long> traversal = __.count().is(GValue.of(GVALUE_NAME, VALUE)).asAdmin();
        assertNotNull(((IsStepPlaceholder) traversal.getSteps().get(1)).getPredicateGValueSafe());
        verifySingleUnpinnedVariable(traversal, GVALUE_NAME);
    }

    @Test
    public void getPredicateShouldPinVariable() {
        final GraphTraversal.Admin<Object, Long> traversal = __.count().is(P.gt(GValue.of(GVALUE_NAME, VALUE))).asAdmin();
        assertNotNull(((IsStepPlaceholder) traversal.getSteps().get(1)).getPredicate());
        verifySinglePinnedVariable(traversal, GVALUE_NAME);
    }

    @Test
    public void getPredicateFromConcreteStep() {
        final GraphTraversal.Admin<Object, Long> traversal = __.count().is(P.gt(GValue.of(GVALUE_NAME, VALUE))).asAdmin();
        assertEquals(VALUE, ((IsStepPlaceholder) traversal.getSteps().get(1)).asConcreteStep().getPredicate().getValue());
    }
}
