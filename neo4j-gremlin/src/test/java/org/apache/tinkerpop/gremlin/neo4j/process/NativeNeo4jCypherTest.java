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
package org.apache.tinkerpop.gremlin.neo4j.process;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.neo4j.AbstractNeo4jGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NativeNeo4jCypherTest extends AbstractNeo4jGremlinTest {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldExecuteCypher() throws Exception {
        this.graph.addVertex("name", "marko");
        this.graph.tx().commit();
        final Iterator<Map<String, Object>> result = this.getGraph().cypher("MATCH (a {name:\"marko\"}) RETURN a", Collections.emptyMap());
        assertNotNull(result);
        assertTrue(result.hasNext());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldExecuteCypherWithArgs() throws Exception {
        this.graph.addVertex("name", "marko");
        this.graph.tx().commit();
        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("n", "marko");
        final Iterator<Map<String, Object>> result = this.getGraph().cypher("MATCH (a {name:{n}}) RETURN a", bindings);
        assertNotNull(result);
        assertTrue(result.hasNext());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldExecuteCypherWithArgsUsingVertexIdList() throws Exception {
        final Vertex v = this.graph.addVertex("name", "marko");
        final List<Object> idList = Arrays.asList(v.id());
        this.graph.tx().commit();

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("ids", idList);
        final Iterator<String> result = this.getGraph().cypher("START n=node({ids}) RETURN n", bindings).select("n").values("name");
        assertNotNull(result);
        assertTrue(result.hasNext());
        assertEquals("marko", result.next());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldExecuteCypherAndBackToGremlin() throws Exception {
        this.graph.addVertex("name", "marko", "age", 29, "color", "red");
        this.graph.addVertex("name", "marko", "age", 30, "color", "yellow");

        this.graph.tx().commit();
        final Traversal result = this.getGraph().cypher("MATCH (a {name:\"marko\"}) RETURN a").select("a").has("age", 29).values("color");
        assertNotNull(result);
        assertTrue(result.hasNext());
        assertEquals("red", result.next().toString());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldExecuteMultiIdWhereCypher() throws Exception {
        this.graph.addVertex("name", "marko", "age", 29, "color", "red");
        this.graph.addVertex("name", "marko", "age", 30, "color", "yellow");
        this.graph.addVertex("name", "marko", "age", 30, "color", "orange");
        this.graph.tx().commit();

        final List<Object> result = this.getGraph().cypher("MATCH n WHERE id(n) IN [1,2] RETURN n").select("n").id().toList();
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(1l));
        assertTrue(result.contains(2l));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldExecuteMultiIdWhereWithParamCypher() throws Exception {
        final Vertex v1 = this.graph.addVertex("name", "marko", "age", 29, "color", "red");
        final Vertex v2 = this.graph.addVertex("name", "marko", "age", 30, "color", "yellow");
        this.graph.addVertex("name", "marko", "age", 30, "color", "orange");
        this.graph.tx().commit();

        final List<Object> ids = Arrays.asList(v1.id(), v2.id());
        final Map<String, Object> m = new HashMap<>();
        m.put("ids", ids);
        final List<Object> result = this.getGraph().cypher("MATCH n WHERE id(n) IN {ids} RETURN n", m).select("n").id().toList();
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(v1.id()));
        assertTrue(result.contains(v2.id()));
    }

}