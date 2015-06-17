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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TinkerGraphTest {

    @Test
    @Ignore
    public void testPlay() {
        Graph g = TinkerGraph.open();
        Vertex v1 = g.addVertex(T.id, "1", "animal", "males");
        Vertex v2 = g.addVertex(T.id, "2", "animal", "puppy");
        Vertex v3 = g.addVertex(T.id, "3", "animal", "mama");
        Vertex v4 = g.addVertex(T.id, "4", "animal", "puppy");
        Vertex v5 = g.addVertex(T.id, "5", "animal", "chelsea");
        Vertex v6 = g.addVertex(T.id, "6", "animal", "low");
        Vertex v7 = g.addVertex(T.id, "7", "animal", "mama");
        Vertex v8 = g.addVertex(T.id, "8", "animal", "puppy");
        Vertex v9 = g.addVertex(T.id, "9", "animal", "chula");

        v1.addEdge("link", v2, "weight", 2f);
        v2.addEdge("link", v3, "weight", 3f);
        v2.addEdge("link", v4, "weight", 4f);
        v2.addEdge("link", v5, "weight", 5f);
        v3.addEdge("link", v6, "weight", 1f);
        v4.addEdge("link", v6, "weight", 2f);
        v5.addEdge("link", v6, "weight", 3f);
        v6.addEdge("link", v7, "weight", 2f);
        v6.addEdge("link", v8, "weight", 3f);
        v7.addEdge("link", v9, "weight", 1f);
        v8.addEdge("link", v9, "weight", 7f);

        g.traversal().withSack(Float.MIN_VALUE).V(v1).repeat(outE().sack(Operator.max, "weight").inV()).times(5).sack().forEachRemaining(System.out::println);
    }

   /* @Test
    public void testTraversalDSL() throws Exception {
        Graph g = TinkerFactory.createClassic();
        assertEquals(2, g.of(TinkerFactory.SocialTraversal.class).people("marko").knows().name().toList().size());
        g.of(TinkerFactory.SocialTraversal.class).people("marko").knows().name().forEachRemaining(name -> assertTrue(name.equals("josh") || name.equals("vadas")));
        assertEquals(1, g.of(TinkerFactory.SocialTraversal.class).people("marko").created().name().toList().size());
        g.of(TinkerFactory.SocialTraversal.class).people("marko").created().name().forEachRemaining(name -> assertEquals("lop", name));
    }*/

    @Test
    @Ignore
    public void benchmarkStandardTraversals() throws Exception {
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = graph.traversal();
        graph.io(GraphMLIo.build()).readGraph("data/grateful-dead.xml");
        final List<Supplier<Traversal>> traversals = Arrays.asList(
                () -> g.V().outE().inV().outE().inV().outE().inV(),
                () -> g.V().out().out().out(),
                () -> g.V().out().out().out().path(),
                () -> g.V().repeat(out()).times(2),
                () -> g.V().repeat(out()).times(3),
                () -> g.V().local(out().out().values("name").fold()),
                () -> g.V().out().local(out().out().values("name").fold()),
                () -> g.V().out().map(v -> g.V(v.get()).out().out().values("name").toList())
        );
        traversals.forEach(traversal -> {
            System.out.println("\nTESTING: " + traversal.get());
            for (int i = 0; i < 7; i++) {
                final long t = System.currentTimeMillis();
                traversal.get().iterate();
                System.out.print("   " + (System.currentTimeMillis() - t));
            }
        });
    }

    @Test
    @Ignore
    public void testPlay4() throws Exception {
        Graph graph = TinkerGraph.open();
        graph.io(GraphMLIo.build()).readGraph("/Users/marko/software/tinkerpop/tinkerpop3/data/grateful-dead.xml");
        GraphTraversalSource g = graph.traversal();
        final List<Supplier<Traversal>> traversals = Arrays.asList(
                () -> g.V().has(T.label, "song").out().groupCount().<Vertex>by(t ->
                        g.V(t).choose(r -> g.V(r).has(T.label, "artist").hasNext(),
                                in("writtenBy", "sungBy"),
                                both("followedBy")).values("name").next()).fold(),
                () -> g.V().has(T.label, "song").out().groupCount().<Vertex>by(t ->
                        g.V(t).choose(has(T.label, "artist"),
                                in("writtenBy", "sungBy"),
                                both("followedBy")).values("name").next()).fold(),
                () -> g.V().has(T.label, "song").out().groupCount().by(
                        choose(has(T.label, "artist"),
                                in("writtenBy", "sungBy"),
                                both("followedBy")).values("name")).fold(),
                () -> g.V().has(T.label, "song").both().groupCount().<Vertex>by(t -> g.V(t).both().values("name").next()),
                () -> g.V().has(T.label, "song").both().groupCount().by(both().values("name")));
        traversals.forEach(traversal -> {
            System.out.println("\nTESTING: " + traversal.get());
            for (int i = 0; i < 10; i++) {
                final long t = System.currentTimeMillis();
                traversal.get().iterate();
                //System.out.println(traversal.get().toList());
                System.out.print("   " + (System.currentTimeMillis() - t));
            }
        });
    }

    @Test
    @Ignore
    public void testPlayDK() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        Traversal traversal = g.V().both().both().count();
        //traversal.forEachRemaining(System.out::println);
        traversal.asAdmin().applyStrategies();
        System.out.println(traversal.toString());
        System.out.println(traversal.toString());
    }

    @Test
    @Ignore
    public void testPlay7() throws Exception {
        Graph graph = TinkerGraph.open();
        graph.io(GraphMLIo.build()).readGraph("/Users/marko/software/tinkerpop/tinkerpop3/data/grateful-dead.xml");
        GraphTraversalSource g = graph.traversal(GraphTraversalSource.standard());
        //System.out.println(g.E().label().groupCount().next());
        final Supplier<Traversal<?, ?>> traversal = () ->
                /*g.V().match("a",
                        as("a").in("sungBy").as("b"),
                        as("a").in("writtenBy").as("b")).select().by("name");*/
                /*g.V().xmatch("a",
                        as("a").out("followedBy").as("b"),
                        as("b").out("followedBy").as("a")).select().by("name");*/
                /*g.V().xmatch("a",
                        as("a").in("followedBy").as("b"),
                        as("a").out("sungBy").as("c"),
                        as("a").out("writtenBy").as("d")).select().by("name");*/
                /*g.V().match("a",
                        as("a").in("followedBy").as("b"),
                        as("a").out("sungBy").as("c"),
                        as("a").out("writtenBy").as("d")).
                        where("c", P.neq("d")).select().by("name");*/
                /*g.V().xmatch("a",
                        as("a").in("sungBy").as("b"),
                        as("a").in("writtenBy").as("b"),
                        as("b").out("followedBy").as("c"),
                        as("c").out("sungBy").as("a"),
                        as("c").out("writtenBy").as("a")).select().by("name");*/
                g.V().xmatch("a",
                        as("a").has("name","Garcia"),
                        as("a").in("writtenBy").as("b"),
                        as("b").out("followedBy").as("c"),
                        as("c").out("writtenBy").has("name",P.neq("Garcia")).as("d")).select().by("name");

        System.out.println(TimeUtil.clockWithResult(100, () -> traversal.get().toList().size()));
    }

    @Test
    @Ignore
    public void testPlay5() throws Exception {
        GraphTraversalSource g = TinkerFactory.createModern().traversal(GraphTraversalSource.standard());
        /*final Supplier<Traversal<?, ?>> traversal = () -> g.V().xmatch("a",
                as("a").out("created").as("b"),
                or(
                        as("a").out("knows").as("c"),
                        or(
                                as("a").out("created").has("name", "ripple"),
                                as("a").out().out()
                        )
                )).select().by("name");*/

        /*final Supplier<Traversal<?, ?>> traversal = () -> g.V().
                xmatch("a",
                as("a").local(out("created").count()).as("b"))
                .select().by("name");*/

        /*final Supplier<Traversal<?, ?>> traversal = () ->
                g.V().xmatch("a",
                        where("a", P.neq("c")),
                        as("a").out("created").as("b"),
                        or(
                                as("a").out("knows").has("name", "vadas"),
                                as("a").in("knows")
                        ),
                        as("b").in("created").as("c"),
                        as("b").where(in("created").count().is(P.gt(1))))
                        .select();*/

        /*final Supplier<Traversal<?,?>> traversal = () ->
                g.V().xmatch("a",
                        as("a").out("knows").count().as("b"),
                        as("a").out("knows").as("c"),
                        as("c").out("created").count().as("b")).select("a","b","c").by("name").by().by("name");*/

       final Supplier<Traversal<?,?>> traversal = () ->
                g.V().as("a").out().as("b").where(
                        in("created").as("a")
                ).select().by("name");

       /* final Supplier<Traversal<?,?>> traversal = () ->
                g.V().as("a").out().as("b").in().as("c").where(P.neq("a")).select().by("name"); */

        System.out.println(traversal.get());
        System.out.println(traversal.get().iterate());
        traversal.get().forEachRemaining(System.out::println);

        //System.out.println(g.V().and(out("created"),or(out("knows"),out("created").has("name","ripple"))).values("name").iterate());
        //g.V().and(out("created"),or(out("knows"),out("created").has("name","ripple"))).values("name").forEachRemaining(System.out::println);
    }

    @Test
    @Ignore
    public void testPlay6() throws Exception {
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource g = graph.traversal(GraphTraversalSource.computer());
        for (int i = 0; i < 1000; i++) {
            graph.addVertex(T.label, "person", T.id, i);
        }
        graph.vertices().forEachRemaining(a -> {
            graph.vertices().forEachRemaining(b -> {
                if (a != b) {
                    a.addEdge("knows", b);
                }
            });
        });
        graph.vertices(50).next().addEdge("uncle", graph.vertices(70).next());
        System.out.println(TimeUtil.clockWithResult(100, () -> g.V().xmatch("a", as("a").out("knows").as("b"), as("a").out("uncle").as("b")).toList()));
    }

    @Test
    public void shouldManageIndices() {
        final TinkerGraph g = TinkerGraph.open();

        Set<String> keys = g.getIndexedKeys(Vertex.class);
        assertEquals(0, keys.size());
        keys = g.getIndexedKeys(Edge.class);
        assertEquals(0, keys.size());

        g.createIndex("name1", Vertex.class);
        g.createIndex("name2", Vertex.class);
        g.createIndex("oid1", Edge.class);
        g.createIndex("oid2", Edge.class);

        // add the same one twice to check idempotance
        g.createIndex("name1", Vertex.class);

        keys = g.getIndexedKeys(Vertex.class);
        assertEquals(2, keys.size());
        for (String k : keys) {
            assertTrue(k.equals("name1") || k.equals("name2"));
        }

        keys = g.getIndexedKeys(Edge.class);
        assertEquals(2, keys.size());
        for (String k : keys) {
            assertTrue(k.equals("oid1") || k.equals("oid2"));
        }

        g.dropIndex("name2", Vertex.class);
        keys = g.getIndexedKeys(Vertex.class);
        assertEquals(1, keys.size());
        assertEquals("name1", keys.iterator().next());

        g.dropIndex("name1", Vertex.class);
        keys = g.getIndexedKeys(Vertex.class);
        assertEquals(0, keys.size());

        g.dropIndex("oid1", Edge.class);
        keys = g.getIndexedKeys(Edge.class);
        assertEquals(1, keys.size());
        assertEquals("oid2", keys.iterator().next());

        g.dropIndex("oid2", Edge.class);
        keys = g.getIndexedKeys(Edge.class);
        assertEquals(0, keys.size());

        g.dropIndex("better-not-error-index-key-does-not-exist", Vertex.class);
        g.dropIndex("better-not-error-index-key-does-not-exist", Edge.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotCreateVertexIndexWithNullKey() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex(null, Vertex.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotCreateEdgeIndexWithNullKey() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex(null, Edge.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotCreateVertexIndexWithEmptyKey() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("", Vertex.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotCreateEdgeIndexWithEmptyKey() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("", Edge.class);
    }

    @Ignore
    @Test
    public void shouldUpdateVertexIndicesInNewGraph() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("name", Vertex.class);

        g.addVertex("name", "marko", "age", 29);
        g.addVertex("name", "stephen", "age", 35);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is used because only "stephen" ages should pass through the pipeline due to the inclusion of the
        // key index lookup on "name".  If there's an age of something other than 35 in the pipeline being evaluated
        // then something is wrong.
        assertEquals(new Long(1), g.traversal().V().has("age", P.test((t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35)).has("name", "stephen").count().next());
    }

    @Ignore
    @Test
    public void shouldRemoveAVertexFromAnIndex() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("name", Vertex.class);

        g.addVertex("name", "marko", "age", 29);
        g.addVertex("name", "stephen", "age", 35);
        final Vertex v = g.addVertex("name", "stephen", "age", 35);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is used because only "stephen" ages should pass through the pipeline due to the inclusion of the
        // key index lookup on "name".  If there's an age of something other than 35 in the pipeline being evaluated
        // then something is wrong.
        assertEquals(new Long(2), g.traversal().V().has("age", P.test((t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35)).has("name", "stephen").count().next());

        v.remove();
        assertEquals(new Long(1), g.traversal().V().has("age", P.test((t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35)).has("name", "stephen").count().next());
    }

    @Ignore
    @Test
    public void shouldUpdateVertexIndicesInExistingGraph() {
        final TinkerGraph g = TinkerGraph.open();

        g.addVertex("name", "marko", "age", 29);
        g.addVertex("name", "stephen", "age", 35);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is not used because "stephen" and "marko" ages both pass through the pipeline.
        assertEquals(new Long(1), g.traversal().V().has("age", P.test((t, u) -> {
            assertTrue(t.equals(35) || t.equals(29));
            return true;
        }, 35)).has("name", "stephen").count().next());

        g.createIndex("name", Vertex.class);

        // another spy into the pipeline for index check.  in this case, we know that at index
        // is used because only "stephen" ages should pass through the pipeline due to the inclusion of the
        // key index lookup on "name".  If there's an age of something other than 35 in the pipeline being evaluated
        // then something is wrong.
        assertEquals(new Long(1), g.traversal().V().has("age", P.test((t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35)).has("name", "stephen").count().next());
    }

    @Ignore
    @Test
    public void shouldUpdateEdgeIndicesInNewGraph() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("oid", Edge.class);

        final Vertex v = g.addVertex();
        v.addEdge("friend", v, "oid", "1", "weight", 0.5f);
        v.addEdge("friend", v, "oid", "2", "weight", 0.6f);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is used because only oid 1 should pass through the pipeline due to the inclusion of the
        // key index lookup on "oid".  If there's an weight of something other than 0.5f in the pipeline being
        // evaluated then something is wrong.
        assertEquals(new Long(1), g.traversal().E().has("weight", P.test((t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5)).has("oid", "1").count().next());
    }

    @Ignore
    @Test
    public void shouldRemoveEdgeFromAnIndex() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("oid", Edge.class);

        final Vertex v = g.addVertex();
        v.addEdge("friend", v, "oid", "1", "weight", 0.5f);
        final Edge e = v.addEdge("friend", v, "oid", "1", "weight", 0.5f);
        v.addEdge("friend", v, "oid", "2", "weight", 0.6f);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is used because only oid 1 should pass through the pipeline due to the inclusion of the
        // key index lookup on "oid".  If there's an weight of something other than 0.5f in the pipeline being
        // evaluated then something is wrong.
        assertEquals(new Long(2), g.traversal().E().has("weight", P.test((t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5)).has("oid", "1").count().next());

        e.remove();
        assertEquals(new Long(1), g.traversal().E().has("weight", P.test((t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5)).has("oid", "1").count().next());
    }

    @Ignore
    @Test
    public void shouldUpdateEdgeIndicesInExistingGraph() {
        final TinkerGraph g = TinkerGraph.open();

        final Vertex v = g.addVertex();
        v.addEdge("friend", v, "oid", "1", "weight", 0.5f);
        v.addEdge("friend", v, "oid", "2", "weight", 0.6f);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is not used because "1" and "2" weights both pass through the pipeline.
        assertEquals(new Long(1), g.traversal().E().has("weight", P.test((t, u) -> {
            assertTrue(t.equals(0.5f) || t.equals(0.6f));
            return true;
        }, 0.5)).has("oid", "1").count().next());

        g.createIndex("oid", Edge.class);

        // another spy into the pipeline for index check.  in this case, we know that at index
        // is used because only oid 1 should pass through the pipeline due to the inclusion of the
        // key index lookup on "oid".  If there's an weight of something other than 0.5f in the pipeline being
        // evaluated then something is wrong.
        assertEquals(new Long(1), g.traversal().E().has("weight", P.test((t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5)).has("oid", "1").count().next());
    }


}
