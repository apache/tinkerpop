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
        g.V().match("a", "d",
                GraphTraversal.of().as("a").out("created").as("b"),
                GraphTraversal.of().as("b").has("name", "lop"),
                GraphTraversal.of().as("b").in("created").as("c"),
                GraphTraversal.of().as("c").has("age", 29),
                GraphTraversal.of().as("c").out("knows").as("d"))
                .select(As.of("a", "d"), v -> ((Vertex) v).getValue("name")).forEach(System.out::println);

        System.out.println("--------------");

        g.V().match("a", "c",
                GraphTraversal.of().as("a").out("created").as("b"),
                GraphTraversal.of().as("a").out("knows").as("b"),
                GraphTraversal.of().as("b").identity().as("c"))
                .value("name").path().forEach(System.out::println);

        System.out.println("--------------");

        g.V().match("a", "b",
                GraphTraversal.of().as("a").out("knows").has("name", "josh"),
                GraphTraversal.of().as("a").out("created").has("name", "lop"),
                GraphTraversal.of().as("a").out("created").as("b"),
                GraphTraversal.of().as("b").has("lang", "java"),
                GraphTraversal.of().as("b").in("created").has("name", "peter"))
                .value("name").path().forEach(System.out::println);
    }
}
