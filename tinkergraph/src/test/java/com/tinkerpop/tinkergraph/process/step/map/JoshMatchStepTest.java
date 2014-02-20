package com.tinkerpop.tinkergraph.process.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.util.As;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JoshMatchStepTest {

    @Test
    public void forJosh() {
        Graph g = TinkerFactory.createClassic();
        g.V().match("a", "d",
                Traversal.of().as("a").out("created").as("b"),
                Traversal.of().as("b").has("name", "lop"),
                Traversal.of().as("b").in("created").as("c"),
                Traversal.of().as("c").has("age", 29),
                Traversal.of().as("c").out("knows").as("d"))
                .select(As.of("a", "d"), v -> ((Vertex) v).getValue("name")).forEach(System.out::println);

        System.out.println("--------------");

        g.V().match("a", "c",
                Traversal.of().as("a").out("created").as("b"),
                Traversal.of().as("a").out("knows").as("b"),
                Traversal.of().as("b").identity().as("c"))
                .value("name").path().forEach(System.out::println);

        System.out.println("--------------");

        g.V().match("a", "b",
                Traversal.of().as("a").out("knows").has("name", "josh"),
                Traversal.of().as("a").out("created").has("name", "lop"),
                Traversal.of().as("a").out("created").as("b"),
                Traversal.of().as("b").has("lang", "java"),
                Traversal.of().as("b").in("created").has("name", "peter"))
                .value("name").path().forEach(System.out::println);
    }
}
