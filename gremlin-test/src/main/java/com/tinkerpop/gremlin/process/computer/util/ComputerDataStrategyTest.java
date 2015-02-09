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
package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.strategy.StrategyGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ComputerDataStrategyTest extends AbstractGremlinTest {

    @Test
    public void shouldFilterHiddenProperties() {
        final StrategyGraph sg = g.strategy(new ComputerDataStrategy(new HashSet<>(Arrays.asList("***hidden-guy"))));

        final Vertex v = sg.addVertex("***hidden-guy", "X", "not-hidden-guy", "Y");
        final Iterator<VertexProperty<String>> props = v.iterators().propertyIterator();
        final VertexProperty v1 = props.next();
        assertEquals("Y", v1.value());
        assertEquals("not-hidden-guy", v1.key());
        assertFalse(props.hasNext());

        final Iterator<String> values = v.iterators().valueIterator();
        assertEquals("Y", values.next());
        assertFalse(values.hasNext());
    }

    @Test
    public void shouldAccessHiddenProperties() {
        final StrategyGraph sg = g.strategy(new ComputerDataStrategy(new HashSet<>(Arrays.asList("***hidden-guy"))));

        final Vertex v = sg.addVertex("***hidden-guy", "X", "not-hidden-guy", "Y");
        final Iterator<VertexProperty<String>> props = v.iterators().propertyIterator("***hidden-guy");
        final VertexProperty<String> v1 = props.next();
        assertEquals("X", v1.value());
        assertEquals("***hidden-guy", v1.key());
        assertFalse(props.hasNext());

        final Iterator<String> values = v.iterators().valueIterator("***hidden-guy");
        assertEquals("X", values.next());
        assertFalse(values.hasNext());
    }

    @Test
    public void shouldHideHiddenKeys() {
        final StrategyGraph sg = g.strategy(new ComputerDataStrategy(new HashSet<>(Arrays.asList("***hidden-guy"))));

        final Vertex v = sg.addVertex("***hidden-guy", "X", "not-hidden-guy", "Y");
        final Set<String> keys = v.keys();
        assertTrue(keys.contains("not-hidden-guy"));
        assertFalse(keys.contains("***hidden-guy"));
    }
}
