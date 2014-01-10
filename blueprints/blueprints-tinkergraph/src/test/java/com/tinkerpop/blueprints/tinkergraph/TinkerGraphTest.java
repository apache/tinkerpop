package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTest {

    @Test
    public void testTinkerGraph() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("name", Vertex.class);
        final Vertex marko = g.addVertex("name", "marko", "age", 33, "blah", "bloop");
        final Vertex stephen = g.addVertex("name", "stephen", "id", 12, "blah", "bloop");
        stephen.setProperty(Property.Key.hidden("name"), "stephen");
        assertEquals("stephen", stephen.getProperty(Property.Key.hidden("name")).get());
        final Random r = new Random();
        Stream.generate(() -> g.addVertex(r.nextBoolean() + "1", r.nextInt(), "name", r.nextInt())).limit(100000).count();
        assertEquals(100002, g.vertices.size());
        Edge edge = marko.addEdge("knows", stephen);
        System.out.println(g.query().has("name", Compare.EQUAL, "marko").vertices());
        System.out.println(marko.query().direction(Direction.OUT).labels("knows", "workedWith").vertices());
        g.createIndex("blah", Vertex.class);

        edge.setProperty("weight", 1.0f);
        edge.setProperty("creator", "stephen");
        assertEquals(edge.getValue("weight"), Float.valueOf(1.0f));
        assertEquals(edge.getProperty("creator").get(), "stephen");
    }

    @Test
    public void testAnnotatedList() {
        final TinkerGraph g = TinkerGraph.open();
        Vertex marko = g.addVertex();
        marko.setProperty("names", AnnotatedList.make());
        System.out.println(marko.getProperty("names"));
        marko.<AnnotatedList>getProperty("names").get().addValue("mArKo", "time", 1);
        marko.<AnnotatedList>getProperty("names").get().addValue("mrodriguez", "time", 2);
        marko.<AnnotatedList>getProperty("names").get().addValue("marko", "time", 3);
        System.out.println(marko.getProperty("names"));


        System.out.println("----");
        marko.<AnnotatedList>getProperty("names").get().query().has("time", Compare.GREATER_THAN, 1).annotatedValues().forEach(System.out::println);
        System.out.println("----");
        marko.<AnnotatedList<String>>getProperty("names").get().query().has("time", 1).values().forEach(System.out::println);

    }
}
