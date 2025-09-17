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
import static org.junit.Assert.assertNotNull;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class TailGlobalStepTest extends GValueStepTest {

    private static final String GVALUE_NAME = "limit";
    private static final long LIMIT_10 = 10L;
    private static final long LIMIT_5 = 5L;

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.tail(LIMIT_5),
                __.tail(LIMIT_10),
                __.tail(GValue.of(GVALUE_NAME, LIMIT_10))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.tail(GValue.of(GVALUE_NAME, LIMIT_10)), Set.of(GVALUE_NAME))
        );
    }

    @Test
    public void getLimitNonGValue() {
        GraphTraversal.Admin<Object, Object> traversal = __.tail(LIMIT_5).asAdmin();
        assertEquals((Long) LIMIT_5, ((TailGlobalStep) traversal.getSteps().get(0)).getLimit());
        verifyNoVariables(traversal);
    }

    @Test
    public void getLimitAsGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Object, Object> traversal = __.tail(GValue.of(GVALUE_NAME, LIMIT_10)).asAdmin();
        assertEquals(GValue.of(GVALUE_NAME, LIMIT_10), ((TailGlobalStepPlaceholder) traversal.getSteps().get(0)).getLimitAsGValue());
        verifySingleUnpinnedVariable(traversal, GVALUE_NAME);
    }

    @Test
    public void getLimitShouldPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.tail(GValue.of(GVALUE_NAME, LIMIT_10)).asAdmin();
        assertNotNull(((TailGlobalStepPlaceholder) traversal.getSteps().get(0)).getLimit());
        verifySinglePinnedVariable(traversal, GVALUE_NAME);
    }
}
