/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.fail;

/**
 * Tests for {@link LabelsDropVerificationStrategy}.
 * Verifies that {@code labels().drop()} patterns are rejected while unrelated patterns are allowed.
 */
@RunWith(Parameterized.class)
public class LabelsDropVerificationStrategyTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // Should REJECT: labels() followed by drop() (possibly with filter steps in between)
                {"labels().drop()", __.labels().drop(), false},
                {"labels().is('x').drop()", __.labels().is("x").drop(), false},
                {"labels().where(P.eq('x')).drop()", __.labels().where(P.eq("x")).drop(), false},
                {"labels().not(__.is('x')).drop()", __.labels().not(__.is("x")).drop(), false},

                // Should ALLOW: no LabelsStep before drop
                {"V().drop()", __.V().drop(), true},
                {"properties().drop()", __.properties().drop(), true},

                // Should ALLOW: non-filter step between labels() and drop() breaks the walk
                {"labels().map(__.constant('x')).drop()", __.labels().map(__.constant("x")).drop(), true},
                {"labels().order().drop()", __.labels().order().drop(), true},
        });
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal.Admin traversal;

    @Parameterized.Parameter(value = 2)
    public boolean allow;

    @Test
    public void shouldVerifyLabelsDropPattern() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(LabelsDropVerificationStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        if (allow) {
            traversal.asAdmin().applyStrategies();
        } else {
            try {
                traversal.asAdmin().applyStrategies();
                fail("The strategy should not allow labels().drop() pattern: " + name);
            } catch (VerificationException ve) {
                // expected
            }
        }
    }
}
