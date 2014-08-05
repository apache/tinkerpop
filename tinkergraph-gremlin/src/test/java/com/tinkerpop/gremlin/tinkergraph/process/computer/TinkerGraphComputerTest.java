package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputerTest {

    @Ignore
    @Test
    public void testStuff() throws Exception {
        Graph g = TinkerFactory.createClassic();
        System.out.println(g.v(1).as("x").out().jump("x", h -> h.getLoops() < 1).<String>value("name").path());
        g.v(1).identity().as("x").out().jump("x", h -> h.getLoops() < 2).<String>value("name").path().submit(g.compute()).forEachRemaining(path -> System.out.println(path));
    }

}
