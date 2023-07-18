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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.CardinalityValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class CardinalityTest {

    @Test
    public void shouldCreateSingle() {
        final CardinalityValueTraversal t = VertexProperty.Cardinality.single("test");
        assertEquals("test", t.getValue());
        assertEquals(VertexProperty.Cardinality.single, t.getCardinality());
    }

    @Test
    public void shouldCreateSet() {
        final CardinalityValueTraversal t = VertexProperty.Cardinality.set("test");
        assertEquals("test", t.getValue());
        assertEquals(VertexProperty.Cardinality.set, t.getCardinality());
    }

    @Test
    public void shouldCreateList() {
        final CardinalityValueTraversal t = VertexProperty.Cardinality.list("test");
        assertEquals("test", t.getValue());
        assertEquals(VertexProperty.Cardinality.list, t.getCardinality());
    }

    @Test
    public void shouldBeEqual() {
        assertEquals(VertexProperty.Cardinality.single("test"), VertexProperty.Cardinality.single("test"));
        assertEquals(VertexProperty.Cardinality.single(1), VertexProperty.Cardinality.single(1));
        assertEquals(VertexProperty.Cardinality.single(null), VertexProperty.Cardinality.single(null));
    }

    @Test
    public void shouldNotBeEqual() {
        assertNotEquals(VertexProperty.Cardinality.single(100), VertexProperty.Cardinality.single("testing"));
        assertNotEquals(VertexProperty.Cardinality.single("test"), VertexProperty.Cardinality.single("testing"));
        assertNotEquals(VertexProperty.Cardinality.single(100), VertexProperty.Cardinality.single(1));
        assertNotEquals(VertexProperty.Cardinality.single("null"), VertexProperty.Cardinality.single(null));
    }
}
