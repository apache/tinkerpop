package com.tinkerpop.gremlin.util.optimizers;

import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityOptimizerTest {

    @Test
    public void shouldRemoveIdentityPipes() {
      /*  Pipeline gremlin = GremlinJ.of(EmptyGraph.instance(), false).identity().identity().identity();
        System.out.println(gremlin);
        assertEquals(3, gremlin.getPipes().size());
        new IdentityOptimizer().optimize(gremlin);
        assertEquals(0, gremlin.getPipes().size());*/
    }

    @Test
    public void shouldNotRemoveAsNamedIdentityPipes() {
      /*  Pipeline gremlin = GremlinJ.of(EmptyGraph.instance(), false).identity().as("x").identity().identity().as("y");
        assertEquals(3, gremlin.getPipes().size());
        new IdentityOptimizer().optimize(gremlin);
        assertEquals(2, gremlin.getPipes().size());
        boolean foundX = false;
        int counter = 0;
        for (final Pipe pipe : (List<Pipe>) gremlin.getPipes()) {
            if (pipe instanceof IdentityPipe)
                assertTrue(!pipe.getAs().startsWith("_"));
            if (pipe.getAs().equals("x")) {
                assertFalse(foundX);
                foundX = true;
                counter++;
            } else if (pipe.getAs().equals("y")) {
                assertTrue(foundX);
                counter++;
            }
        }
        assertEquals(2, counter); */
    }

    @Test
    public void shouldStillMaintainGlobalChain() {
      /*  // NO OPTIMIZER
        Pipeline gremlin = GremlinJ.of(EmptyGraph.instance(), false).identity().as("x").identity().identity().as("y");
        gremlin.addStarts(new SingleIterator<>(new SimpleHolder<>("marko")));
        assertEquals(3, gremlin.getPipes().size());
        assertTrue(gremlin.hasNext());
        assertEquals("marko", gremlin.next());
        assertFalse(gremlin.hasNext());
        for (int i = 0; i < gremlin.getPipes().size(); i++) {
            if (i > 0)
                assertEquals(((Pipe) gremlin.getPipes().get(i)).getPreviousPipe(), gremlin.getPipes().get(i - 1));
            if (i < gremlin.getPipes().size() - 1)
                assertEquals(((Pipe) gremlin.getPipes().get(i)).getNextPipe(), gremlin.getPipes().get(i + 1));
        }

        // WITH OPTIMIZER
        gremlin = GremlinJ.of(EmptyGraph.instance(), false).identity().as("x").identity().identity().as("y");
        assertEquals(3, gremlin.getPipes().size());
        new IdentityOptimizer().optimize(gremlin);
        gremlin.addStarts(new SingleIterator<>(new SimpleHolder<>("marko")));
        assertEquals(2, gremlin.getPipes().size());
        assertTrue(gremlin.hasNext());
        assertEquals("marko", gremlin.next());
        assertFalse(gremlin.hasNext());
        for (int i = 0; i < gremlin.getPipes().size(); i++) {
            if (i > 0)
                assertEquals(((Pipe) gremlin.getPipes().get(i)).getPreviousPipe(), gremlin.getPipes().get(i - 1));
            if (i < gremlin.getPipes().size() - 1)
                assertEquals(((Pipe) gremlin.getPipes().get(i)).getNextPipe(), gremlin.getPipes().get(i + 1));
        }
    } */
    }
}
