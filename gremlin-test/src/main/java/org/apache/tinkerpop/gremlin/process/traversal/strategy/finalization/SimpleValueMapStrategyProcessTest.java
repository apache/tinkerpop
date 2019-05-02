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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public class SimpleValueMapStrategyProcessTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldApplyUnfold() throws Exception {

        //
        // without tokens
        //
        GraphTraversal<Vertex, Map<Object, Object>> t = g.withStrategies(SimpleValueMapStrategy.instance())
                .V().has("person", "name", "marko").valueMap();

        assertTrue(t.hasNext());

        Map<Object, Object> result = t.next();

        assertFalse(t.hasNext());
        assertEquals(2, result.size());
        assertTrue(result.containsKey("name"));
        assertTrue(result.containsKey("age"));
        assertEquals("marko", result.get("name"));
        assertEquals(29, result.get("age"));

        //
        // with tokens
        //
        t = g.withStrategies(SimpleValueMapStrategy.instance())
                .V().has("person", "name", "marko").valueMap().with(WithOptions.tokens);

        assertTrue(t.hasNext());

        result = t.next();

        assertFalse(t.hasNext());
        assertEquals(4, result.size());
        assertTrue(result.containsKey(T.id));
        assertTrue(result.containsKey(T.label));
        assertTrue(result.containsKey("name"));
        assertTrue(result.containsKey("age"));
        assertEquals("person", result.get(T.label));
        assertEquals("marko", result.get("name"));
        assertEquals(29, result.get("age"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotApplyUnfold() throws Exception {

        final List<String> list = Collections.singletonList("X");

        //
        // without tokens
        //
        GraphTraversal<Vertex, Map<Object, Object>> t = g.withStrategies(SimpleValueMapStrategy.instance())
                .V().has("person", "name", "marko").valueMap().by(__.constant(list));

        assertTrue(t.hasNext());

        Map<Object, Object> result = t.next();

        assertFalse(t.hasNext());
        assertEquals(2, result.size());
        assertTrue(result.containsKey("name"));
        assertTrue(result.containsKey("age"));
        assertEquals(list, result.get("name"));
        assertEquals(list, result.get("age"));

        //
        // with tokens
        //
        t = g.withStrategies(SimpleValueMapStrategy.instance())
                .V().has("person", "name", "marko").valueMap().with(WithOptions.tokens).by(__.constant(list));

        assertTrue(t.hasNext());

        result = t.next();

        assertFalse(t.hasNext());
        assertEquals(4, result.size());
        assertTrue(result.containsKey(T.id));
        assertTrue(result.containsKey(T.label));
        assertTrue(result.containsKey("name"));
        assertTrue(result.containsKey("age"));
        assertEquals(list, result.get(T.id));
        assertEquals(list, result.get(T.label));
        assertEquals(list, result.get("name"));
        assertEquals(list, result.get("age"));
    }
}