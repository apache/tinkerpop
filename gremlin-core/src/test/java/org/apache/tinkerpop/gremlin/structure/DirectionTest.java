package org.apache.tinkerpop.gremlin.structure;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DirectionTest {
    @Test
    public void shouldReturnOppositeOfIn() {
        assertEquals(Direction.OUT, Direction.IN.opposite());
    }

    @Test
    public void shouldReturnOppositeOfOut() {
        assertEquals(Direction.IN, Direction.OUT.opposite());
    }

    @Test
    public void shouldReturnOppositeOfBoth() {
        assertEquals(Direction.BOTH, Direction.BOTH.opposite());
    }
}
