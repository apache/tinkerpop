/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.groovy;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyTranslatorTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportStringSupplierLambdas() throws Exception {
        final GraphTraversalSource g = graph.traversal().withTranslator(GroovyTranslator.of("g"));
        GraphTraversal.Admin<Vertex, Integer> t = (GraphTraversal.Admin) g.V().hasLabel("person").out().map(x -> "it.get().value('name').length()").asAdmin();
        List<Integer> lengths = t.toList();
        assertEquals(6, lengths.size());
        assertTrue(lengths.contains(3));
        assertTrue(lengths.contains(4));
        assertTrue(lengths.contains(5));
        assertTrue(lengths.contains(6));
        assertEquals(24, g.V().hasLabel("person").out().map(x -> "it.get().value('name').length()").sum().next().intValue());

        /*t = g.V().hasLabel("person").out().filter(x -> "{ x.sideEffects('lengthSum', x.get().value('name').length()").asAdmin();
        lengths = t.toList();
        assertEquals(6, lengths.size());
        assertTrue(lengths.contains(3));
        assertTrue(lengths.contains(4));
        assertTrue(lengths.contains(5));
        assertTrue(lengths.contains(6));
        assertEquals(24, g.V().hasLabel("person").out().map(x -> "it.get().value('name').length()").sum().next().intValue());*/
    }
}
