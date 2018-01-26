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
package org.apache.tinkerpop.gremlin.sparql.process.traversal.dsl.sparql;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.sparql.process.traversal.strategy.SparqlStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SparqlTraversalSourceTest {

    private static final Graph graph = TinkerFactory.createModern();
    private static final SparqlTraversalSource g = graph.traversal(SparqlTraversalSource.class);
    private static final GraphTraversalSource _g = graph.traversal();

    @Test
    public void shouldFindAllPersonsNamesAndAges() {
        final List<?> x = g.sparql("SELECT ?name ?age WHERE { ?person v:name ?name . ?person v:age ?age }").toList();
        assertThat(x, containsInAnyOrder(
                new HashMap<String,Object>(){{
                    put("name", "marko");
                    put("age", 29);
                }},
                new HashMap<String,Object>(){{
                    put("name", "vadas");
                    put("age", 27);
                }},
                new HashMap<String,Object>(){{
                    put("name", "josh");
                    put("age", 32);
                }},
                new HashMap<String,Object>(){{
                    put("name", "peter");
                    put("age", 35);
                }}
        ));
    }

    @Test
    public void shouldFindAllPersonsNamesAndAgesOrdered() {
        final List<?> x = g.sparql("SELECT ?name ?age WHERE { ?person v:name ?name . ?person v:age ?age } ORDER BY ASC(?age)").toList();
        assertThat(x, contains(
                new HashMap<String,Object>(){{
                    put("name", "vadas");
                    put("age", 27);
                }},
                new HashMap<String,Object>(){{
                    put("name", "marko");
                    put("age", 29);
                }},
                new HashMap<String,Object>(){{
                    put("name", "josh");
                    put("age", 32);
                }},
                new HashMap<String,Object>(){{
                    put("name", "peter");
                    put("age", 35);
                }}
        ));
    }

    @Test
    public void shouldFindAllNamesOrdered() {
        final List<?> x = g.sparql("SELECT ?name WHERE { ?person v:name ?name } ORDER BY DESC(?name)").toList();
        assertThat(x, contains("vadas", "ripple", "peter", "marko", "lop", "josh"));
    }

    @Test
    public void shouldFilter() {
        final Map<String,Vertex> x = (Map) g.sparql(  "SELECT ?a ?b ?c\n" +
                                                            "WHERE {\n" +
                                                            "  ?a v:label \"person\" .\n" +
                                                            "  ?a e:knows ?b .\n" +
                                                            "  ?a e:created ?c .\n" +
                                                            "  ?b e:created ?c .\n" +
                                                            "  ?a v:age ?d .\n" +
                                                            "    FILTER (?d < 30)\n" +
                                                            "}").next();

        assertEquals(x.get("a"), _g.V(1).next());
        assertEquals(x.get("b"), _g.V(4).next());
        assertEquals(x.get("c"), _g.V(3).next());
    }

    @Test
    public void shouldDistinct() {
        final List<?> x = g.sparql(
                "SELECT DISTINCT ?name\n" +
                "WHERE {\n" +
                "    ?a e:created ?b .\n" +
                "    ?a v:name ?name .\n" +
                "}").toList();
        assertThat(x, containsInAnyOrder("marko", "josh", "peter"));
    }

    @Test
    public void shouldDistinctAndOrder() {
        final List<?> x = g.sparql(
                "SELECT DISTINCT ?name\n" +
                        "WHERE {\n" +
                        "    ?a e:created ?b .\n" +
                        "    ?a v:name ?name .\n" +
                        "}" +
                        "ORDER BY ?name").toList();
        assertThat(x, contains("josh", "marko", "peter"));
    }
}
