package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinTest {

    @Test
    public void testToString() {
        System.out.println(Property.empty());
    }

    @Test
    public void testPipeline() {

        TinkerGraph g = TinkerFactory.createClassic();
        Gremlin.of(g).V()
                .out("knows").out("created")
                .has("name")
                .value("name").path()
                .sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        Gremlin.of(g).V().as("x").out("knows").back("x").path().sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        Gremlin.of(g).v("1").as("x").out()
                .jump("x", o -> o.getLoops() < 2)
                .path().sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        System.out.println(Gremlin.of(g).V().both().groupCount());

        System.out.println("--------------");

        Gremlin.of(g).V()
                .both()
                .dedup(e -> e.get().getProperty("name").isPresent())
                .sideEffect(System.out::println)
                .iterate();

        System.out.println("--------------");

    }

    @Test
    public void testSelect() {
        TinkerGraph g = TinkerFactory.createClassic();
        Gremlin.of(g).V().as("x").out().as("y").select("x", "y").sideEffect(System.out::println).iterate();
    }

    @Ignore
    public void testMatch() {
        TinkerGraph g = TinkerFactory.createClassic();
        Gremlin.of(g).V()
                .match("a", "d",
                        Gremlin.of().as("a").out("knows").as("b"),
                        Gremlin.of().as("b").out("created").as("c"),
                        Gremlin.of().as("c").value("name").as("d"))
                .sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        Gremlin.of(g).V()
                .match("a", "b",
                        Gremlin.of().as("a").out("knows").has("name", "josh"),
                        Gremlin.of().as("a").out("created").has("name", "lop"),
                        Gremlin.of().as("a").out("created").as("b"))
                .value("name").path()
                .sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        Gremlin.of(g).V()
                .match("a", "b",
                        Gremlin.of().as("a").out("knows").has("name", "josh"),
                        Gremlin.of().as("a").out("created").has("name", "lop"),
                        Gremlin.of().as("a").out("created").as("b"),
                        Gremlin.of().as("b").has("lang", "java"),
                        Gremlin.of().as("b").in("created").has("name", "peter"))
                .value("name").path()
                .sideEffect(System.out::println).iterate();
    }

    @Test
    public void testLoop() {

        TinkerGraph g = TinkerGraph.open(Optional.empty());
        Vertex a = g.addVertex(Property.Key.ID, "1");
        Vertex b = g.addVertex(Property.Key.ID, "2");
        Vertex c = g.addVertex(Property.Key.ID, "3");
        Vertex d = g.addVertex(Property.Key.ID, "4");
        Vertex e = g.addVertex(Property.Key.ID, "5");
        Vertex f = g.addVertex(Property.Key.ID, "6");
        a.addEdge("next", b);
        b.addEdge("next", c);
        c.addEdge("next", d);
        d.addEdge("next", e);
        e.addEdge("next", f);
        f.addEdge("next", a);

        Gremlin.of(g).v(a.getId()).as("x").out()
                .jump("x", o -> o.getLoops() < 8, o -> true)
                .sideEffect(o -> System.out.println(o.getLoops()))
                .path().sideEffect(System.out::println).iterate();

    }

    @Test
    public void testLoop2() {

        TinkerGraph g = TinkerFactory.createClassic();
        Gremlin.of(g).V().as("x").out().jump("x", o -> o.getLoops() < 2).property("name").forEach(System.out::println);
        Gremlin.of(g).V().as("x").jump("y", o -> o.getLoops() > 1).out().jump("x").property("name").as("y").forEach(System.out::println);
    }

    @Test
    public void testValues() {
        Graph g = TinkerFactory.createClassic();
        //Gremlin.of(g).V().values("name","age","label","id").forEach(System.out::println);
        Pipeline gremlin = Gremlin.of(g).v(1).out("created").aggregate("x").in("created").out("created").except("x").value("name");
        gremlin.forEach(System.out::println);
        System.out.println(((Set<Vertex>) gremlin.get("x").get()).iterator().next().<String>getValue("name"));


    }

    @Test
    public void testRange() {
        Graph graph = TinkerFactory.createClassic();
        Gremlin<Vertex, Vertex> g = (Gremlin) Gremlin.of(graph);
        assertEquals(3l, g.V().range(0, 2).count());

        g.v(1).out().forEach(System.out::println);
        System.out.println(g.v(1).out().tree(o -> ((Vertex) o).getValue("name")));
    }
}
