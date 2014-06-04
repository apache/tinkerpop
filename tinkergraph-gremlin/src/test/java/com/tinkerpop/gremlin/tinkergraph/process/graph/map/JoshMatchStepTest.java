package com.tinkerpop.gremlin.tinkergraph.process.graph.map;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.util.As;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JoshMatchStepTest {

    @Test
    public void forJosh() {
        Graph g = TinkerFactory.createClassic();
        GraphTraversal t = g.V().match("a", "c",
                g.of().as("a").out("created").as("b"),
                g.of().as("b").has("name", "lop"),
                g.of().as("b").in("created").as("c"),
                //g.of().as("a").in("knows").as("c"),
                g.of().as("c").has("age", 29))
                .select(As.of("a", "c"), v -> ((Vertex) v).value("name"));
        t.forEach(System.out::println);
        t.forEach(System.out::println);   // TODO: wouldn't it be nice if GraphTraversal iterators were idempotent?
//        assertEquals(10, t.toList().size());

        System.out.println("--------------");

        g.V().match("a", "c",
                //g.of().as("a").out("created", "knows").as("b"),
                g.of().as("a").out("created").as("b"),
                g.of().as("a").out("knows").as("b"),
                g.of().as("b").identity().as("c"))
                .value("name").path().forEach(System.out::println);

        System.out.println("--------------");

        g.V().match("a", "d",
                g.of().as("a").out("created").as("c"),
                g.of().as("a").has("name", "josh"),
                g.of().as("b").out("created").as("c"),
                // ??? a != b
                g.of().as("c").identity().as("d"))
                .value("name").path().forEach(System.out::println);

        System.out.println("--------------");

        g.V().match("a", "b",
                g.of().as("a").out("knows").has("name", "josh"),
                g.of().as("a").out("created").has("name", "lop"),
                g.of().as("a").out("created").as("b"),
                g.of().as("b").has("lang", "java"),
                g.of().as("b").in("created").has("name", "peter"))
                .value("name").path().forEach(System.out::println);
    }
}
