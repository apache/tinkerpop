package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputerTest {

    @Ignore
    @Test
    public void testStuff() throws Exception {
        Graph g = TinkerFactory.createClassic();

        //ComputeResult result = g.compute().program(TraversalVertexProgram.create().gremlin(() -> g.V().outE()).build()).submit().get();
        //g.V().has("name", "marko").out().value("name").forEach(System.out::println);
        // System.out.println("----------");
        //new TraversalResult<>(g, () -> g.V().out().value("name")).forEachRemaining(System.out::println);

        //g.v(1).as("x").has("name", "marko").out().value("name").back("x").submit(g.compute()).forEachRemaining(System.out::println);

        //System.out.println(g.v(1).as("x").out().jump("x", h -> h.getLoops() < 1).<String>value("name").path().submit(g.compute()));
        System.out.println(g.v(1).as("x").out().jump("x", h -> h.getLoops() < 1).<String>value("name").path());
        g.v(1).identity().as("x").out().jump("x", h -> h.getLoops() < 2).<String>value("name").path().submit(g.compute()).forEachRemaining(path -> System.out.println(path));

        //g.V().pageRank().map(pair -> pair.get().getValue0()).value("name").path().submit(g.compute()).forEachRemaining(System.out::println);

        //System.out.println("----------");

        // g.V().pageRank().order((a, b) -> b.get().getValue1().compareTo(a.get().getValue1())).range(0,2).forEachRemaining(System.out::println);
    }

    @Test
    @Ignore
    public void testOLAPWriteBack() throws Exception {
        Graph g = TinkerFactory.createClassic();
        g.V().pageRank(() -> GraphTraversal.of().out("knows").inE("knows")).forEachRemaining(System.out::println);


        g.V().as("a").out("knows").in("knows").linkIn("knows2", "a").iterate();
        g.E().has(Element.LABEL, Compare.NOT_EQUAL, "knows2").remove();
        g.E().forEach(System.out::println);
        g.V().pageRank(() -> GraphTraversal.of().outE()).forEachRemaining(System.out::println);
    }

    @Test
    public void testPlay() {
        Graph g = TinkerFactory.createClassic();
        g.V().has("name", "josh").out().value("name").reverse().forEachRemaining(System.out::println);
    }

    @Test
    @Ignore
    public void testPlay2() throws Exception {
        Graph g = TinkerGraph.open();
        GraphMLReader reader = GraphMLReader.create().build();
        reader.readGraph(new FileInputStream("data/grateful-dead.xml"), g);
        //g.V().as("x").out().jump("x", t -> t.getLoops() < 2).count().forEachRemaining(System.out::println);
        g.V().as("x").out().jump("x", t -> t.getLoops() < 3).count().submit(g.compute()).forEachRemaining(System.out::println);

        //g.V().out().count().submit(g.compute()).forEachRemaining(System.out::println);
    }
}
