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
public class PropertiesStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.values(),
                __.values("name"),
                __.values("age"),
                __.values("name", "age"),
                __.properties(),
                __.properties("name"),
                __.properties("age"),
                __.properties("name", "age")
        );
    }

    /**
     * We used to have an issue that because we use XOR of vertex ids, properties("x") and properties("x", "y", "y") are considered equal.
     * The same 2 property key "y" are reset as a result of XOR. See: <a href="https://issues.apache.org/jira/browse/TINKERPOP-2423">TINKERPOP-2423</a>
     * This test verifies that the issue was fixed.
     */
    @Test
    public void testCheckEqualityWithRedundantKeys() {
        final Traversal<?, ?> t0 = __.properties("x");
        final Traversal<?, ?> t1 = __.properties("x",  "y", "y");

        final Traversal<?, ?> t2 = __.values("x");
        final Traversal<?, ?> t3 = __.values("x",  "y", "y");

        assertThat(t0.asAdmin().getSteps(), hasSize(1));
        assertThat(t1.asAdmin().getSteps(), hasSize(1));
        assertThat(t2.asAdmin().getSteps(), hasSize(1));
        assertThat(t3.asAdmin().getSteps(), hasSize(1));

        assertThat(t0.asAdmin().getSteps().get(0), instanceOf(PropertiesStep.class));
        assertThat(t1.asAdmin().getSteps().get(0), instanceOf(PropertiesStep.class));
        assertThat(t2.asAdmin().getSteps().get(0), instanceOf(PropertiesStep.class));
        assertThat(t3.asAdmin().getSteps().get(0), instanceOf(PropertiesStep.class));

        final PropertiesStep<?> propertiesStep0 = (PropertiesStep<?>) t0.asAdmin().getSteps().get(0);
        final PropertiesStep<?> propertiesStep1 = (PropertiesStep<?>) t1.asAdmin().getSteps().get(0);
        assertThat("properties(\"x\") and properties(\"x\",\"y\",\"y\") must not be considered equal",
                propertiesStep0, not(equalTo(propertiesStep1)));

        final PropertiesStep<?> propertiesStep2 = (PropertiesStep<?>) t0.asAdmin().getSteps().get(0);
        final PropertiesStep<?> propertiesStep3 = (PropertiesStep<?>) t1.asAdmin().getSteps().get(0);
        assertThat("values(\"x\") and values(\"x\",\"y\",\"y\") must not be considered equal",
                propertiesStep2, not(equalTo(propertiesStep3)));
    }
}
