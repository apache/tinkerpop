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
package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class LocalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.local(out()),
                __.local(out("knows")),
                __.local(in())
        );
    }

    @Test
    public void shouldBeDebulkedToGroupCountSideEffectInLocal() {
        final Traversal t = __.inject(1L, 1L, 2L, 3L).barrier().local(__.groupCount("x").select("x"));
        assertEquals(Map.of(1L, 1L), t.next());
        assertEquals(Map.of(1L, 2L), t.next());
        assertEquals(Map.of(1L, 2L, 2L, 1L), t.next());
        assertEquals(Map.of(1L, 2L, 2L, 1L, 3L, 1L), t.next());
    }

    @Test
    public void shouldBeDebulkedToGroupCountInLocal() {
        final Traversal t = __.inject(1L, 1L, 2L, 3L).barrier().local(__.groupCount());
        assertEquals(Map.of(1L, 1L), t.next());
        assertEquals(Map.of(1L, 1L), t.next());
        assertEquals(Map.of(2L, 1L), t.next());
        assertEquals(Map.of(3L, 1L), t.next());
    }
}
