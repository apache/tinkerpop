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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class VertexStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.out(),
                __.in(),
                __.both(),
                __.out("knows"),
                __.out("created"),
                __.out("knows", "created")
        );
    }

    /**
     * We used to have an issue that because we use XOR of edge labels, for example out("x") and out("x", "y", "y") are considered equal.
     * The same 2 sideEffectKey "y" are reset as a result of XOR. See: <a href="https://issues.apache.org/jira/browse/TINKERPOP-2423">TINKERPOP-2423</a>
     * This test verifies that the issue was fixed.
     */
    @Test
    public void testCheckEqualityWithRedundantEdgeLabels() {
        final Traversal<?, ?> t0 = __.both("x");
        final Traversal<?, ?> t1 = __.both("x",  "y", "y");

        assertThat(t0.asAdmin().getSteps(), hasSize(1));
        assertThat(t1.asAdmin().getSteps(), hasSize(1));
        assertThat(t0.asAdmin().getSteps().get(0), instanceOf(VertexStep.class));
        assertThat(t1.asAdmin().getSteps().get(0), instanceOf(VertexStep.class));

        final VertexStep<?> vertexStep0 = (VertexStep<?>) t0.asAdmin().getSteps().get(0);
        final VertexStep<?> vertexStep1 = (VertexStep<?>) t1.asAdmin().getSteps().get(0);
        assertThat("both(\"x\") and both(\"x\",\"y\",\"y\") must not be considered equal", vertexStep0, not(equalTo(vertexStep1)));
    }
}
