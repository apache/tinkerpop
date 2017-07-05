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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class HasStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.has("name"),
                __.has("name", "marko"),
                __.has("name", out("knows").values("name")),
                __.hasId(1),
                __.hasId(1.0),
                __.hasKey("name"),
                __.hasKey("age"),
                __.hasLabel("person"),
                __.hasLabel("project"),
                __.hasNot("name"),
                __.hasNot("age"),
                __.hasValue("marko"),
                __.hasValue("josh")
        );
    }

    /**
     * This test ensures that `has[Id|Label|Key|Value]` are compatible with the old varargs method signatures.
     */
    @Test
    public void testVarargsCompatibility() {
        final List<List<Traversal>> traversalLists = Arrays.asList(
                // hasId(Object id, Object... moreIds) should be compatible with hasId(Object... ids)
                Arrays.asList(
                        __.hasId(1),
                        __.hasId(eq(1)),
                        __.hasId(new Integer[]{1})),
                Arrays.asList(
                        __.hasId(1, 2),
                        __.hasId(within(1, 2)),
                        __.hasId(new Integer[]{1, 2})),

                // hasLabel(Object label, Object... moreLabels) should be compatible with hasLabel(Object... labels)
                Arrays.asList(
                        __.hasLabel("person"),
                        __.hasLabel(eq("person"))),
                Arrays.asList(
                        __.hasLabel("person", "software"),
                        __.hasLabel(within("person", "software"))),

                // hasKey(Object key, Object... moreKeys) should be compatible with hasKey(Object... keys)
                Arrays.asList(
                        __.hasKey("name"),
                        __.hasKey(eq("name"))),
                Arrays.asList(
                        __.hasKey("name", "age"),
                        __.hasKey(within("name", "age"))),

                // hasValue(Object value, Object... moreValues) should be compatible with hasValue(Object... values)
                Arrays.asList(
                        __.hasValue("marko"),
                        __.hasValue(eq("marko")),
                        __.hasValue(new String[]{"marko"})),
                Arrays.asList(
                        __.hasValue("marko", 32),
                        __.hasValue(within("marko", 32)),
                        __.hasValue(new Object[]{"marko", 32}))
        );
        
        for (final List<Traversal> traversals : traversalLists) {
            for (Traversal traversal1 : traversals) {
                final Step step1 = traversal1.asAdmin().getEndStep();
                assertEquals(step1, step1.clone());
                assertEquals(step1.hashCode(), step1.clone().hashCode());
                for (Traversal traversal2 : traversals) {
                    final Step step2 = traversal2.asAdmin().getEndStep();
                    assertEquals(step1, step2);
                    assertEquals(step1.hashCode(), step2.hashCode());
                }
            }
        }
    }
}
