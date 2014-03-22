package com.tinkerpop.gremlin.tinkergraph.process.olap;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputerTest {

    @Ignore
    @Test
    public void testStuff() throws Exception {
        Graph g = TinkerFactory.createClassic();

        //ComputeResult result = g.compute().program(TraversalVertexProgram.create().gremlin(() -> g.V().outE()).build()).submit().get();
        g.V().has("name", "marko").out().value("name").forEach(System.out::println);
        System.out.println("----------");
        //new TraversalResult<>(g, () -> g.V().out().value("name")).forEachRemaining(System.out::println);

        g.V().has("name", "marko").out().value("name").submit(g.compute()).forEachRemaining(System.out::println);

        //g.V().pageRank().map(pair -> pair.get().getValue0()).value("name").path().submit(g.compute()).forEachRemaining(System.out::println);

        //System.out.println("----------");

        // g.V().pageRank().order((a, b) -> b.get().getValue1().compareTo(a.get().getValue1())).range(0,2).forEachRemaining(System.out::println);
    }

    @Ignore
    @Test
    public void testOLAPWriteBack() throws Exception {
        Graph g = TinkerFactory.createClassic();

        //Graph g = TinkerGraph.open();
        //new KryoReader.Builder(g).build().readGraph(new FileInputStream("gremlin/data/graph-example-2.gio"));
        //g.V().value("name").forEach(System.out::println);

        //g.V().pageRank().sideEffect(p -> p.get().getValue0().setProperty("pageRank", p.get().getValue1())).forEachRemaining(System.out::println);
        //System.out.println("---------------");
        g.V().has("name")
                .pageRank()
                .order((a, b) -> a.get().getValue1().compareTo(b.get().getValue1()))
                .map(p -> Arrays.asList(p.get().getValue0().<String>getValue("name"), p.get().getValue1()))
                .forEachRemaining(System.out::println);

    }
}
