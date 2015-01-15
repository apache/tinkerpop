package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Operator;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.gremlin.structure.strategy.PartitionStrategy;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTest {

    private static String tempPath;

    static {
        final String temp = System.getProperty("java.io.tmpdir", File.separator + "tmp").trim();
        if (!temp.endsWith(File.separator))
            tempPath = temp + File.separator;
        else
            tempPath = temp;

        tempPath = tempPath + "tinkerpop-io/";
    }

    @BeforeClass
    public static void before() throws IOException {
        final File tempDir = new File(tempPath);
        FileUtils.deleteDirectory(tempDir);
        if (!tempDir.mkdirs()) throw new IOException(String.format("Could not create %s", tempDir));
    }

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

        v1.withSack(() -> Float.MIN_VALUE).repeat(__.outE().sack(Operator.max, "weight").inV()).times(5).sack().submit(g.compute()).forEachRemaining(System.out::println);
    }

    @Test
    public void testTraversalDSL() throws Exception {
        Graph g = TinkerFactory.createClassic();
        assertEquals(2, g.of(TinkerFactory.SocialTraversal.class).people("marko").knows().name().toList().size());
        g.of(TinkerFactory.SocialTraversal.class).people("marko").knows().name().forEachRemaining(name -> assertTrue(name.equals("josh") || name.equals("vadas")));
        assertEquals(1, g.of(TinkerFactory.SocialTraversal.class).people("marko").created().name().toList().size());
        g.of(TinkerFactory.SocialTraversal.class).people("marko").created().name().forEachRemaining(name -> assertEquals("lop", name));
    }

    @Test
    @Ignore
    public void testPlay2() throws Exception {
        Graph g = TinkerGraph.open();
        g.io().readGraphML("/Users/marko/software/tinkerpop/tinkerpop3/data/grateful-dead.xml");
        for (int i = 0; i < 100; i++) {
            final long t = System.currentTimeMillis();
            g.V().outE().inV().outE().inV().outE().inV().iterate();
            System.out.println(System.currentTimeMillis() - t);
        }
        // System.out.println(t);
        // t.forEachRemaining(System.out::println);
        // System.out.println(t);
    }

    @Test
    @Ignore
    public void testPlay3() throws Exception {
        Graph g = TinkerFactory.createModern();
        g = g.strategy(PartitionStrategy.build().partitionKey("name").create());
        GraphTraversal t = g.V().out();
        System.out.println(t.toString());
        t.iterate();
        System.out.println(t.toString());

    }

    @Test
    @Ignore
    public void testPlay4() throws Exception {
        Graph g = TinkerFactory.createModern();
        g.V().choose(v -> v.label().equals("person"),
                __.union(__.out().<String>values("lang"), __.out().<String>values("name")),
                __.in().label()).map(s -> s.get() + "^^").submit(g.compute()).forEachRemaining(System.out::println);
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsKryo() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic.gio");
        KryoWriter.build().create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsKryo() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern.gio");
        KryoWriter.build().create().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewGraphAsKryo() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-crew.gio");
        KryoWriter.build().create().writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicVerticesAsKryo() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic-vertices.gio");
        KryoWriter.build().create().writeVertices(os, TinkerFactory.createClassic().V(), Direction.BOTH);
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicVerticesAsGraphSON() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic-vertices.ldjson");
        GraphSONWriter.build().create().writeVertices(os, TinkerFactory.createClassic().V(), Direction.BOTH);
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernVerticesAsKryo() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern-vertices.gio");
        KryoWriter.build().create().writeVertices(os, TinkerFactory.createModern().V(), Direction.BOTH);
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernVerticesAsGraphSON() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern-vertices.ldjson");
        GraphSONWriter.build().create().writeVertices(os, TinkerFactory.createModern().V(), Direction.BOTH);
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewVerticesAsKryo() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-crew-vertices.gio");
        KryoWriter.build().create().writeVertices(os, TinkerFactory.createTheCrew().V(), Direction.BOTH);
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphML() throws IOException {
        try (final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic.xml")) {
            GraphMLWriter.build().create().writeGraph(os, TinkerFactory.createClassic());
        }
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphML() throws IOException {
        try (final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern.xml")) {
            GraphMLWriter.build().create().writeGraph(os, TinkerFactory.createModern());
        }
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphSONNoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic.json");
        GraphSONWriter.build().create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphSONNoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern.json");
        GraphSONWriter.build().create().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewGraphAsGraphSONNoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-crew.json");
        GraphSONWriter.build().create().writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphNormalizedAsGraphSON() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic-normalized.json");
        GraphSONWriter.build().mapper(GraphSONMapper.build().normalize(true).create()).create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphNormalizedAsGraphSON() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern-normalized.json");
        GraphSONWriter.build().mapper(GraphSONMapper.build().normalize(true).create()).create().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphSONWithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic-typed.json");
        GraphSONWriter.build().mapper(GraphSONMapper.build().embedTypes(true).create())
                .create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphSONWithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern-typed.json");
        GraphSONWriter.build().mapper(GraphSONMapper.build().embedTypes(true).create())
                .create().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewGraphAsGraphSONWithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-crew-typed.json");
        GraphSONWriter.build().mapper(GraphSONMapper.build().embedTypes(true).create())
                .create().writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
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
        assertEquals(1, StreamFactory.stream(g.V().has("age", (t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35).has("name", "stephen")).count());
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
        assertEquals(2, StreamFactory.stream(g.V().has("age", (t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35).has("name", "stephen")).count());

        v.remove();
        assertEquals(1, StreamFactory.stream(g.V().has("age", (t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35).has("name", "stephen")).count());
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
        assertEquals(1, StreamFactory.stream(g.V().has("age", (t, u) -> {
            assertTrue(t.equals(35) || t.equals(29));
            return true;
        }, 35).has("name", "stephen")).count());

        g.createIndex("name", Vertex.class);

        // another spy into the pipeline for index check.  in this case, we know that at index
        // is used because only "stephen" ages should pass through the pipeline due to the inclusion of the
        // key index lookup on "name".  If there's an age of something other than 35 in the pipeline being evaluated
        // then something is wrong.
        assertEquals(1, StreamFactory.stream(g.V().has("age", (t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35).has("name", "stephen")).count());
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
        assertEquals(1, StreamFactory.stream(g.E().has("weight", (t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5).has("oid", "1")).count());
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
        assertEquals(2, StreamFactory.stream(g.E().has("weight", (t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5).has("oid", "1")).count());

        e.remove();
        assertEquals(1, StreamFactory.stream(g.E().has("weight", (t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5).has("oid", "1")).count());
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
        assertEquals(1, StreamFactory.stream(g.E().has("weight", (t, u) -> {
            assertTrue(t.equals(0.5f) || t.equals(0.6f));
            return true;
        }, 0.5).has("oid", "1")).count());

        g.createIndex("oid", Edge.class);

        // another spy into the pipeline for index check.  in this case, we know that at index
        // is used because only oid 1 should pass through the pipeline due to the inclusion of the
        // key index lookup on "oid".  If there's an weight of something other than 0.5f in the pipeline being
        // evaluated then something is wrong.
        assertEquals(1, StreamFactory.stream(g.E().has("weight", (t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5).has("oid", "1")).count());
    }

    /**
     * This test helps with data conversions on Grateful Dead.  No Assertions...run as needed. Never read from the
     * GraphML source as it will always use a String identifier.
     */
    @Test
    public void shouldWriteGratefulDead() throws IOException {
        final Graph g = TinkerGraph.open();
        final GraphReader reader = KryoReader.build().create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/com/tinkerpop/gremlin/structure/io/kryo/grateful-dead.gio")) {
            reader.readGraph(stream, g);
        }

        /* keep this hanging around because changes to kryo format will need grateful dead generated from json so you can generate the gio
        final GraphReader reader = GraphSONReader.build().embedTypes(true).create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/com/tinkerpop/gremlin/structure/io/graphson/grateful-dead.json")) {
            reader.readGraph(stream, g);
        }
        */

        final Graph ng = TinkerGraph.open();
        g.V().sideEffect(ov -> {
            final Vertex v = ov.get();
            if (v.label().equals("song"))
                ng.addVertex(T.id, Integer.parseInt(v.id().toString()), T.label, "song", "name", v.value("name"), "performances", v.property("performances").orElse(0), "songType", v.property("songType").orElse(""));
            else if (v.label().equals("artist"))
                ng.addVertex(T.id, Integer.parseInt(v.id().toString()), T.label, "artist", "name", v.value("name"));
            else
                throw new RuntimeException("damn");
        }).iterate();

        g.E().sideEffect(oe -> {
            final Edge e = oe.get();
            final Vertex v2 = ng.V(Integer.parseInt(e.inV().next().id().toString())).next();
            final Vertex v1 = ng.V(Integer.parseInt(e.outV().next().id().toString())).next();

            if (e.label().equals("followedBy"))
                v1.addEdge("followedBy", v2, T.id, Integer.parseInt(e.id().toString()), "weight", e.value("weight"));
            else if (e.label().equals("sungBy"))
                v1.addEdge("sungBy", v2, T.id, Integer.parseInt(e.id().toString()));
            else if (e.label().equals("writtenBy"))
                v1.addEdge("writtenBy", v2, T.id, Integer.parseInt(e.id().toString()));
            else
                throw new RuntimeException("bah");

        }).iterate();

        final OutputStream os = new FileOutputStream(tempPath + "grateful-dead.gio");
        KryoWriter.build().create().writeGraph(os, ng);
        os.close();

        final OutputStream os2 = new FileOutputStream(tempPath + "grateful-dead.json");
        GraphSONWriter.build().mapper(GraphSONMapper.build().embedTypes(true).create()).create().writeGraph(os2, g);
        os2.close();

        final OutputStream os3 = new FileOutputStream(tempPath + "grateful-dead.xml");
        GraphMLWriter.build().create().writeGraph(os3, g);
        os3.close();

        final OutputStream os4 = new FileOutputStream(tempPath + "grateful-dead-vertices.gio");
        KryoWriter.build().create().writeVertices(os4, g.V(), Direction.BOTH);
        os.close();

        final OutputStream os5 = new FileOutputStream(tempPath + "grateful-dead-vertices.ldjson");
        GraphSONWriter.build().create().writeVertices(os5, g.V(), Direction.BOTH);
        os.close();
    }

    protected void deleteFile(final String path) throws IOException {
        final File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }
}
