package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.util.As;
import org.junit.Test;

import java.util.Collection;
import java.util.Optional;

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
        GremlinJ.of(g).V()
                .out("knows").out("created")
                .has("name")
                .value("name").path()
                .sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        GremlinJ.of(g).V().as("x").out("knows").back("x").path().sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        GremlinJ.of(g).v("1").as("x").out()
                .jump("x", o -> o.getLoops() < 2)
                .path().sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        System.out.println(GremlinJ.of(g).V().both().groupCount());

        System.out.println("--------------");

        GremlinJ.of(g).V()
                .both()
                .dedup(e -> e.getProperty("name").isPresent())
                .sideEffect(System.out::println)
                .iterate();

        System.out.println("--------------");

    }

    @Test
    public void testSelect() {
        TinkerGraph g = TinkerFactory.createClassic();
        GremlinJ.of(g)
                .V().as("x")
                .out().as("y")
                .select(As.of("x", "y"), v -> ((Vertex) v).getValue("name"))
                .sideEffect(System.out::println).iterate();
    }

    @Test
    public void testMatch() {
        TinkerGraph g = TinkerFactory.createClassic();
        GremlinJ.of(g).V()
                .match("a", "d",
                        GremlinJ.of().as("a").out("created").as("b"),
                        GremlinJ.of().as("b").has("name", "lop"),
                        GremlinJ.of().as("b").in("created").as("c"),
                        GremlinJ.of().as("c").has("age", 29),
                        GremlinJ.of().as("c").out("knows").as("d"))
                .select(As.of("a", "d"), v -> ((Vertex) v).getValue("name")).forEach(System.out::println);

        System.out.println("--------------");

        GremlinJ.of(g).V()
                .match("a", "c",
                        GremlinJ.of().as("a").out("created").as("b"),
                        GremlinJ.of().as("a").out("knows").as("b"),
                        GremlinJ.of().as("b").identity().as("c"))
                .value("name").path()
                .sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        GremlinJ.of(g).V()
                .match("a", "b",
                        GremlinJ.of().as("a").out("knows").has("name", "josh"),
                        GremlinJ.of().as("a").out("created").has("name", "lop"),
                        GremlinJ.of().as("a").out("created").as("b"),
                        GremlinJ.of().as("b").has("lang", "java"),
                        GremlinJ.of().as("b").in("created").has("name", "peter"))
                .value("name").path()
                .sideEffect(System.out::println).iterate();
    }

    @Test
    public void testLoop() {

        TinkerGraph g = TinkerGraph.open(Optional.empty());
        Vertex a = g.addVertex(Element.ID, "1");
        Vertex b = g.addVertex(Element.ID, "2");
        Vertex c = g.addVertex(Element.ID, "3");
        Vertex d = g.addVertex(Element.ID, "4");
        Vertex e = g.addVertex(Element.ID, "5");
        Vertex f = g.addVertex(Element.ID, "6");
        a.addEdge("next", b);
        b.addEdge("next", c);
        c.addEdge("next", d);
        d.addEdge("next", e);
        e.addEdge("next", f);
        f.addEdge("next", a);

        GremlinJ.of(g).v(a.getId()).as("x").out()
                .jump("x", o -> o.getLoops() < 8, o -> true)
                .sideEffect(o -> System.out.println(o.getLoops()))
                .path().sideEffect(System.out::println).iterate();

    }

    @Test
    public void testLoop2() {
        TinkerGraph g = TinkerFactory.createClassic();
        GremlinJ.of(g).V().as("x").out().jump("x", o -> o.getLoops() < 2).property("name").forEach(System.out::println);
        GremlinJ.of(g).V().as("x").jump("y", o -> o.getLoops() > 1).out().jump("x").property("name").as("y").forEach(System.out::println);
    }

    @Test
    public void testValues() {
        Graph g = TinkerFactory.createClassic();
        //Gremlin.of(g).V().values("name","age","label","id").forEach(System.out::println);
        Pipeline gremlin = GremlinJ.of(g).v(1).out("created").aggregate("x").in("created").out("created").except("x").value("name");
        gremlin.forEach(System.out::println);
        System.out.println(((Collection<Vertex>) gremlin.memory().get("x")).iterator().next().<String>getValue("name"));


    }

    @Test
    public void testRange() {
        Graph graph = TinkerFactory.createClassic();
        GremlinJ<Vertex, Vertex> g = (GremlinJ) GremlinJ.of(graph);
        assertEquals(3l, g.V().range(0, 2).count());

        //g.v(1).out().forEach(System.out::println);
        System.out.println(g.v(1).out().tree(o -> ((Vertex) o).getValue("name")));

        GremlinJ.of(graph).V().out().remove();
        System.out.println(graph);
    }

    @Test
    public void testOrder() {
        Graph g = TinkerFactory.createClassic();
        GremlinJ.of(g).V().<String>value("name").order((a, b) -> b.get().compareTo(a.get())).path().forEach(System.out::println);


    }

    @Test
    public void testUnion() {
        Graph g = TinkerFactory.createClassic();
        /*Gremlin.of(g).v(1).as("x").union(
                Gremlin.of().out("knows"),
                Gremlin.of().out("created").in("created")
        ).jump("x", h -> h.getLoops() < 2).value("name").path().forEach(System.out::println);*/

        System.out.println(GremlinJ.of(g).V().identity().as("x").has("age").value("age"));
    }

    @Test
    public void testIntersect() {
        Graph g = TinkerFactory.createClassic();
        /*Gremlin.of(g).V().intersect(
                Gremlin.of().out("knows"),
                Gremlin.of().out("created")
        ).path(v -> ((Vertex) v).getValue("name")).forEach(System.out::println);*/

        GremlinJ.of(g).V().match("a", "b",
                GremlinJ.of().as("a").out("knows").as("c"),
                GremlinJ.of().as("a").out("created").as("c"),
                GremlinJ.of().as("c").identity().as("b")
        ).path(v -> ((Vertex) v).getValue("name")).forEach(System.out::println);

    }

    @Test
    public void testAnnotatedList() {
        Graph g = TinkerFactory.createModern();
        Pipeline gremlin = GremlinJ.of(g).V().value("locations").dedup();
        System.out.println(gremlin);
        gremlin.forEach(System.out::println);


    }

    @Test
    public void testDedupOptimizer() {
        Graph g = TinkerFactory.createModern();
        Pipeline gremlin = GremlinJ.of(g).V().value("name").order().dedup();
        gremlin.forEach(System.out::println);
        System.out.println(gremlin);

        // g.v(1).values().has().has()
    }
}