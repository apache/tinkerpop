package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CoreTraversalTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldAddStartsProperly() {
        final Traversal<Object, Vertex> traversal = g.of().out().out();
        assertFalse(traversal.hasNext());
        traversal.addStarts(new TraverserIterator(traversal.getSteps().get(0), false, g.V()));
        assertTrue(traversal.hasNext());
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            counter++;
        }
        assertEquals(2, counter);

        traversal.addStarts(new TraverserIterator(traversal.getSteps().get(0), false, g.V()));
        traversal.addStarts(new TraverserIterator(traversal.getSteps().get(0), false, g.V()));
        counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(4, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldTraversalResetProperly() {
        final Traversal<Object, Vertex> traversal = g.of().as("a").out().out().<Vertex>has("name", Contains.within, Arrays.asList("ripple", "lop")).as("b");
        if (new Random().nextBoolean()) traversal.reset();
        assertFalse(traversal.hasNext());
        traversal.addStarts(new TraverserIterator(traversal.getSteps().get(0), false, g.V()));
        assertTrue(traversal.hasNext());
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            counter++;
        }
        assertEquals(2, counter);

        if (new Random().nextBoolean()) traversal.reset();
        traversal.addStarts(new TraverserIterator(traversal.getSteps().get(0), false, g.V()));
        assertTrue(traversal.hasNext());
        traversal.next();
        assertTrue(traversal.hasNext());
        traversal.reset();
        assertFalse(traversal.hasNext());

        traversal.addStarts(new TraverserIterator(traversal.getSteps().get(0), false, g.V()));
        counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(2, counter);

        assertFalse(traversal.hasNext());
        if (new Random().nextBoolean()) traversal.reset();
        assertFalse(traversal.hasNext());
    }
}
