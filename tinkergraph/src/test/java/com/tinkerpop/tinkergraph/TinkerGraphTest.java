package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StreamFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTest {

    @Test
    public void shouldPerformBasicTinkerGraphIntegrationTest() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("name", Vertex.class);
        final Vertex marko = g.addVertex("name", "marko", "age", 33, "blah", "bloop");
        final Vertex stephen = g.addVertex("name", "stephen", "id", 12, "blah", "bloop");
        stephen.setProperty(Property.Key.hidden("name"), "stephen");
        assertEquals("stephen", stephen.getProperty(Property.Key.hidden("name")).get());
        final Random r = new Random();
        Stream.generate(() -> g.addVertex(r.nextBoolean() + "1", r.nextInt(), "name", r.nextInt())).limit(100000).count();
        assertEquals(100002, g.vertices.size());
        final Edge edge = marko.addEdge("knows", stephen);
        //System.out.println(g.V().has("name", Compare.EQUAL, "marko"));
        //System.out.println(marko.out("knows", "workedWith").toString());
        g.createIndex("blah", Vertex.class);

        edge.setProperty("weight", 1.0f);
        edge.setProperty("creator", "stephen");
        assertEquals(edge.getValue("weight"), Float.valueOf(1.0f));
        assertEquals(edge.getProperty("creator").get(), "stephen");

        //g.V().out().value("name").path().map(p -> p.get().getAsLabels()).forEach(System.out::println);
        //marko.out().path().map(p -> p.get().getAsLabels()).forEach(System.out::println);
    }

    @Test
    public void testPlay() {
        final TinkerGraph g = TinkerFactory.createClassic();
        //System.out.println(g.v(1).outE().as("here").inV().has("name","vadas").back("here").toList());


        System.out.println(g.V().out().getSteps());
        Traversal t = g.V().out();
        TraversalHelper.removeStep((Step)t.getSteps().get(1),t);
        System.out.println(t);
        System.out.println(((Step) t.getSteps().get(0)).getNextStep());
        System.out.println(((Step) t.getSteps().get(0)).getPreviousStep());
    }

    @Test
    public void shouldValidateAnnotatedList() {
        final TinkerGraph g = TinkerGraph.open();
        final Vertex marko = g.addVertex();
        marko.setProperty("names", AnnotatedList.make());
        final Property<AnnotatedList<String>> names = marko.getProperty("names");
        System.out.println(names.get().addValue("marko", "time", 1));
        names.get().addValue("antonio", "time", 2);
        names.get().addValue("mrodriguez", "time", 7);
        System.out.println(names);
        System.out.println("-------");
        names.get().query().has("time", 2).values().forEach(System.out::println);
        System.out.println("-------");
        names.get().query().has("time", 1).annotatedValues().forEach(a -> a.remove());
        names.get().query().values().forEach(System.out::println);
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
        final String location = "/tmp/tp/tinkergraph-serialization-test";
        deleteFile(location);

        final File f = new File(location);
        if (!f.exists())
            f.mkdirs();

        final ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(location + "/tinkergraph"));
        out.writeObject(g);
        out.close();

        final ObjectInputStream input = new ObjectInputStream(new FileInputStream(location + "/tinkergraph"));

        try {
            final TinkerGraph g1 = (TinkerGraph) input.readObject();

            // todo: should test this more and consider strategy carefully
            assertEquals(g.V().count(), g1.V().count());
            assertEquals(g.E().count(), g1.E().count());
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
