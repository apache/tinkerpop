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
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.where;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NativeNeo4jCypherCheck extends AbstractNeo4jGremlinTest {
    private static final Logger logger = LoggerFactory.getLogger(NativeNeo4jCypherCheck.class);

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

    @Test
    @Ignore
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    public void benchmarkCypherAndMatch() throws Exception {
        final Neo4jGraph n = (Neo4jGraph) graph;
        final List<Supplier<GraphTraversal<?, ?>>> traversals = Arrays.asList(
                () -> g.V().match(
                        as("a").in("sungBy").as("b"),
                        as("a").in("writtenBy").as("b")).select("a","b").by("name"),
                () -> n.cypher("MATCH (a)<-[:sungBy]-(b), (a)<-[:writtenBy]-(b) RETURN a, b").select("a","b").by("name"),
                ///
                () -> g.V().match(
                        as("a").out("followedBy").as("b"),
                        as("b").out("followedBy").as("a")).select("a","b").by("name"),
                () -> n.cypher("MATCH (a)-[:followedBy]->(b), (b)-[:followedBy]->(a) RETURN a, b").select("a","b").by("name"),
                ///
                () -> g.V().match(
                        as("a").out("followedBy").count().as("b"),
                        as("a").in("followedBy").count().as("b"),
                        as("b").is(P.gt(10))).select("a").by("name"),
                () -> n.cypher("MATCH (a)-[:followedBy]->(b) WITH a, COUNT(b) AS bc WHERE bc > 10 MATCH (a)<-[:followedBy]-(c) WITH a, bc, COUNT(c) AS cc WHERE bc = cc RETURN a").select("a").by("name"),
                ///
                () -> g.V().match(
                        as("a").in("sungBy").count().as("b"),
                        as("a").in("sungBy").as("c"),
                        as("c").out("followedBy").as("d"),
                        as("d").out("sungBy").as("e"),
                        as("e").in("sungBy").count().as("b"),
                        where("a", P.neq("e"))).select("a", "e").by("name"),
                () -> n.cypher("MATCH (a)<-[:sungBy]-()-[:followedBy]->()-[:sungBy]->(e) WHERE a <> e WITH a, e MATCH (a)<-[:sungBy]-(b) WITH a, e, COUNT(DISTINCT b) as bc MATCH (e)<-[:sungBy]-(f) WITH a, e, bc, COUNT(DISTINCT f) as fc WHERE bc = fc RETURN a, e").select("a", "e").by("name"),
                ///
                () -> g.V().match(
                        as("a").in("followedBy").as("b"),
                        as("a").out("sungBy").as("c"),
                        as("a").out("writtenBy").as("d")).select("a","b","c","d").by("name"),
                () -> n.cypher("MATCH (a)<-[:followedBy]-(b), (a)-[:sungBy]->(c), (a)-[:writtenBy]->(d) RETURN a, b, c, d").select("a","b","c","d").by("name"),
                ///
                () -> g.V().match(
                        as("a").in("followedBy").as("b"),
                        as("a").out("sungBy").as("c"),
                        as("a").out("writtenBy").as("d"),
                        where("c", P.neq("d"))).select("a","b","c","d").by("name"),
                () -> n.cypher("MATCH (a)<-[:followedBy]-(b), (a)-[:sungBy]->(c), (a)-[:writtenBy]->(d) WHERE c <> d RETURN a, b, c, d").select("a","b","c","d").by("name"),
                ///
                () -> g.V().match(
                        as("a").in("sungBy").as("b"),
                        as("a").in("writtenBy").as("b"),
                        as("b").out("followedBy").as("c"),
                        as("c").out("sungBy").as("a"),
                        as("c").out("writtenBy").as("a")).select("a", "b", "c").by("name"),
                () -> n.cypher("MATCH (a)<-[:sungBy]-(b), (a)<-[:writtenBy]-(b), (b)-[:followedBy]->(c), (c)-[:sungBy]->(a), (c)-[:writtenBy]->(a) RETURN a, b, c").select("a", "b", "c").by("name"),
                ///
                () -> g.V().match(
                        as("a").has("name", "Garcia").has(T.label, "artist"),
                        as("a").in("writtenBy").as("b"),
                        as("b").out("followedBy").as("c"),
                        as("c").out("writtenBy").as("d"),
                        as("d").where(P.neq("a"))).select("a","b","c","d").by("name"),
                () -> n.cypher("MATCH (a)<-[:writtenBy]-(b), (b)-[:followedBy]->(c), (c)-[:writtenBy]->(d) WHERE a <> d AND a.name = 'Garcia' AND 'artist' IN labels(a) RETURN a, b, c, d").select("a","b","c","d").by("name"),
                ///
                () -> g.V().match(
                        as("a").out("followedBy").as("b"),
                        as("a").has(T.label, "song").has("performances", P.gt(10)),
                        as("a").out("writtenBy").as("c"),
                        as("b").out("writtenBy").as("c")).select("a", "b", "c").by("name"),
                () -> n.cypher("MATCH (a)-[:followedBy]->(b), (a)-[:writtenBy]->(c), (b)-[:writtenBy]->(c) WHERE a.performances > 10 AND 'song' IN labels(a) RETURN a, b, c").select("a","b","c").by("name")
        );
        int counter = 0;
        for (final Supplier<GraphTraversal<?, ?>> traversal : traversals) {
            logger.info("pre-strategy:  {}", traversal.get());
            logger.info("post-strategy: {}", traversal.get().iterate());
            logger.info(TimeUtil.clockWithResult(25, () -> traversal.get().count().next()).toString());
            if (++counter % 2 == 0)
                logger.info("------------------");
        }
    }

}