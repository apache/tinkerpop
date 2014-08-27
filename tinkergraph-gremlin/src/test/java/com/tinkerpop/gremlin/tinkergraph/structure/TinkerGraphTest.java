package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Direction;
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
import java.util.Set;
import java.util.UUID;

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
        g.of(TinkerFactory.SocialTraversal.class).people("marko").knows().name().forEach(name -> assertTrue(name.equals("josh") || name.equals("vadas")));
        g.of(TinkerFactory.SocialTraversal.class).people("marko").created().name().forEach(name -> assertEquals("lop", name));
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
    public void shouldWriteClassicVerticesAsKryo() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic-vertices.gio");
        KryoWriter.build().create().writeVertices(os, TinkerFactory.createClassic().V(), Direction.BOTH);
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernVerticesAsKryoAsKryo() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern-vertices.gio");
        final TinkerGraph g = TinkerFactory.createModern();
        KryoWriter.build().create().writeVertices(os, g.V(), Direction.BOTH);
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
    public void shouldWriteClassicGraphNormalizedAsGraphSON() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic-normalized.json");
        GraphSONWriter.build().normalize(true).create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphNormalizedAsGraphSON() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern-normalized.json");
        GraphSONWriter.build().normalize(true).create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphSONWithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-classic-typed.json");
        GraphSONWriter.build().embedTypes(true)
                .create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphSONWithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(tempPath + "tinkerpop-modern-typed.json");
        GraphSONWriter.build().embedTypes(true)
                .create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteGratefulGraphAsKryo() throws IOException {
        final Graph g = TinkerGraph.open();
        final GraphReader reader = GraphMLReader.build().create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/com/tinkerpop/gremlin/structure/util/io/graphml/grateful-dead.xml")) {
            reader.readGraph(stream, g);
        }

        final OutputStream os = new FileOutputStream(tempPath + "grateful-dead.gio");
        KryoWriter.build().create().writeGraph(os, g);
        os.close();
    }

    @Test
    public void shouldWriteGratefulGraphAsGraphSON() throws IOException {
        final Graph g = TinkerGraph.open();
        final GraphReader reader = GraphMLReader.build().create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/com/tinkerpop/gremlin/structure/util/io/graphml/grateful-dead.xml")) {
            reader.readGraph(stream, g);
        }

        final OutputStream os = new FileOutputStream(tempPath + "grateful-dead.json");
        GraphSONWriter.build().embedTypes(true).create().writeGraph(os, g);
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
