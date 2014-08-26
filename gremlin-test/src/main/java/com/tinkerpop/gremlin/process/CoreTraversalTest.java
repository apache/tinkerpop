package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC_DOUBLE;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CoreTraversalTest extends AbstractGremlinProcessTest {

    @Test
    @Ignore
    @LoadGraphWith(CLASSIC_DOUBLE)
    public void shouldResetProperly() {
        final Traversal<Object, Vertex> traversal = g.of().out().out();
        assertFalse(traversal.hasNext());
        traversal.addStarts(new TraverserIterator(null, false, g.V()));
        assertTrue(traversal.hasNext());
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
        }
        assertEquals(2, counter);
        traversal.reset();
        traversal.addStarts(new TraverserIterator(null, false, g.V()));
        assertTrue(traversal.hasNext());
        //traversal.next();
        //assertTrue(traversal.hasNext());
        // traversal.reset();
        //assertFalse(traversal.hasNext());

        /*traversal.addStarts(new TraverserIterator(TraversalHelper.getStart(traversal),false, g.V()));
        counter = 0;
        while(traversal.hasNext()){
            counter++;
            traversal.next();
        }
        assertEquals(2, counter); */
    }
}
