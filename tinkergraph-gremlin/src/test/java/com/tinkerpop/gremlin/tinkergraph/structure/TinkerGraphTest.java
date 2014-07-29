package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.IoTest;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTest implements Serializable {

    private static String tempPath;

    static {
        final String temp = System.getProperty("java.io.tmpdir", File.separator + "tmp").trim();
        if (!temp.endsWith(File.separator))
            tempPath = temp + File.separator;
        else
            tempPath = temp;
    }

    @Test
    public void testTraversalDSL() throws Exception {
        Graph g = TinkerFactory.createClassic();
        g.traversal(TinkerFactory.SocialTraversal.class).people("marko").knows().name().forEach(System.out::println);

        /*Graph h = g.compute().program(PageRankVertexProgram.create().create()).submit().get().getValue0();
        System.out.println(h.v(1).value(PageRankVertexProgram.PAGE_RANK).toString());
        Map<String, String> map = new HashMap<>();
        map.put(PageRankVertexProgram.PAGE_RANK, "pageRank");
        TinkerGraphComputer.mergeComputedView(g, h, map);
        System.out.println(g.v(1).value("pageRank").toString());*/
    }

    @Test
    public void shouldPerformBasicTinkerGraphIntegrationTest() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("name", Vertex.class);
        final Vertex marko = g.addVertex("name", "marko", "age", 33, "blah", "bloop");
        final Vertex stephen = g.addVertex("name", "stephen", "id", 12, "blah", "bloop");
        stephen.property(Graph.Key.hidden("name"), "stephen");
        assertEquals("stephen", stephen.property(Graph.Key.hidden("name")).value());
        final Random r = new Random();
        Stream.generate(() -> g.addVertex(r.nextBoolean() + "1", r.nextInt(), "name", r.nextInt())).limit(100000).count();
        assertEquals(100002, g.vertices.size());
        final Edge edge = marko.addEdge("knows", stephen);
        //System.out.println(g.V().has("name", Compare.EQUAL, "marko"));
        //System.out.println(marko.out("knows", "workedWith").toString());
        g.createIndex("blah", Vertex.class);

        edge.property("weight", 1.0f);
        edge.property("creator", "stephen");
        assertEquals(edge.value("weight"), Float.valueOf(1.0f));
        assertEquals(edge.property("creator").value(), "stephen");

        //g.V().out().value("name").path().map(p -> p.get().getAsLabels()).forEach(System.out::println);
        //marko.out().path().map(p -> p.get().getAsLabels()).forEach(System.out::println);
    }

    @Test
    public void testPlay() throws Exception {
        final TinkerGraph g = TinkerFactory.createClassic();
        /*Traversal traversal = g.V().both().value("name").groupCount("a").path();
        TinkerHelper.prepareTraversalForComputer(traversal);
        Pair<Graph,GraphComputer.Globals> result = g.compute().program(TraversalVertexProgram.create().traversal(() -> traversal).create()).submit().get();
        System.out.println("" + result.getValue1().get("a"));*/

        Traversal traversal = g.V().out().out().submit(g.compute()).in().value("name");
        traversal.forEachRemaining(System.out::println);
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsKryoAsKryo() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic.gio");
        KryoWriter.create().build().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphML() throws IOException {
        try (final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic.xml")) {
            GraphMLWriter.create().build().writeGraph(os, TinkerFactory.createClassic());
        }
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphSONNoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic.json");
        GraphSONWriter.create().build().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphNormalizedAsGraphSON() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic-normalized.json");
        GraphSONWriter.create().normalize(true).build().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphSONWithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic-typed.json");
        GraphSONWriter.create().embedTypes(true)
                .build().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }


    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphSONNoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern.json");
        GraphSONWriter.create().build().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphSONWithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern-typed.json");
        GraphSONWriter.create().embedTypes(true)
                .build().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphNormalizedAsGraphSON() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern-normalized.json");
        GraphSONWriter.create().normalize(true).build().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteGratefulGraphAsKryo() throws IOException {
        final Graph g = TinkerGraph.open();
        final GraphReader reader = GraphMLReader.create().build();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/com/tinkerpop/gremlin/structure/util/io/graphml/grateful-dead.xml")) {
            reader.readGraph(stream, g);
        }

        final OutputStream os = new FileOutputStream(tempPath + "grateful-dead.gio");
        KryoWriter.create().build().writeGraph(os, g);
        os.close();
    }


    @Test
    public void shouldWriteGratefulGraphAsGraphSON() throws IOException {
        final Graph g = TinkerGraph.open();
        final GraphReader reader = GraphMLReader.create().build();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/com/tinkerpop/gremlin/structure/util/io/graphml/grateful-dead.xml")) {
            reader.readGraph(stream, g);
        }

        final OutputStream os = new FileOutputStream(tempPath + "grateful-dead.json");
        GraphSONWriter.create().embedTypes(true).build().writeGraph(os, g);
        os.close();
    }


    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsKryo() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern.gio");
        KryoWriter.create().build().writeGraph(os, TinkerFactory.createModern());
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

    @Test
    public void shouldSerializeGraph() throws Exception {
        final TinkerGraph g = TinkerFactory.createClassic();
        final String fileName = UUID.randomUUID().toString() + ".bin";
        final String location = tempPath + "tp" + File.separator + "tinkergraph-serialization-test" + File.separator;
        deleteFile(location);

        final File f = new File(location);
        if (!f.exists())
            f.mkdirs();


        final ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(location + fileName));
        out.writeObject(g);
        out.close();

        final ObjectInputStream input = new ObjectInputStream(new FileInputStream(location + fileName));

        try {
            final TinkerGraph g1 = (TinkerGraph) input.readObject();
            IoTest.assertClassicGraph(g1, false, false);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        } finally {
            input.close();
        }
    }

    protected void deleteFile(final String path) throws IOException {
        final File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }
}
