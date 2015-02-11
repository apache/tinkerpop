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
package org.apache.tinkerpop.gremlin.groovy.engine;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyTraversalScriptTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldSubmitTraversalCorrectly() throws Exception {
        final List<String> names = GroovyTraversalScript.<Vertex, String>of("g.V().out().out().values('name')").over(g).using(g.compute()).traversal().get().toList();
        assertEquals(2, names.size());
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("ripple"));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldSubmitTraversalCorrectly2() throws Exception {
        final List<String> names = GroovyTraversalScript.<Vertex, String>of("g.V(1).out().out().values('name')").over(g).using(g.compute()).traversal().get().toList();
        assertEquals(2, names.size());
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("ripple"));
    }
}
