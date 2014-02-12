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
        assertEquals(3, gremlin.getSteps().size());
        new IdentityOptimizer().optimize(gremlin);
        assertEquals(0, gremlin.getSteps().size());*/
    }

    @Test
    public void shouldNotRemoveAsNamedIdentityPipes() {
      /*  Pipeline gremlin = GremlinJ.of(EmptyGraph.instance(), false).identity().as("x").identity().identity().as("y");
        assertEquals(3, gremlin.getSteps().size());
        new IdentityOptimizer().optimize(gremlin);
        assertEquals(2, gremlin.getSteps().size());
        boolean foundX = false;
        int counter = 0;
        for (final Step pipe : (List<Step>) gremlin.getSteps()) {
            if (pipe instanceof IdentityStep)
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
        assertEquals(3, gremlin.getSteps().size());
        assertTrue(gremlin.hasNext());
        assertEquals("marko", gremlin.next());
        assertFalse(gremlin.hasNext());
        for (int i = 0; i < gremlin.getSteps().size(); i++) {
            if (i > 0)
                assertEquals(((Step) gremlin.getSteps().get(i)).getPreviousStep(), gremlin.getSteps().get(i - 1));
            if (i < gremlin.getSteps().size() - 1)
                assertEquals(((Step) gremlin.getSteps().get(i)).getNextStep(), gremlin.getSteps().get(i + 1));
        }

        // WITH OPTIMIZER
        gremlin = GremlinJ.of(EmptyGraph.instance(), false).identity().as("x").identity().identity().as("y");
        assertEquals(3, gremlin.getSteps().size());
        new IdentityOptimizer().optimize(gremlin);
        gremlin.addStarts(new SingleIterator<>(new SimpleHolder<>("marko")));
        assertEquals(2, gremlin.getSteps().size());
        assertTrue(gremlin.hasNext());
        assertEquals("marko", gremlin.next());
        assertFalse(gremlin.hasNext());
        for (int i = 0; i < gremlin.getSteps().size(); i++) {
            if (i > 0)
                assertEquals(((Step) gremlin.getSteps().get(i)).getPreviousStep(), gremlin.getSteps().get(i - 1));
            if (i < gremlin.getSteps().size() - 1)
                assertEquals(((Step) gremlin.getSteps().get(i)).getNextStep(), gremlin.getSteps().get(i + 1));
        }
    } */
    }
}
