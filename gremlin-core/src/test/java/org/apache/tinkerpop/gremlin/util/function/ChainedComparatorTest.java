package org.apache.tinkerpop.gremlin.util.function;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ChainedComparatorTest {

    final ChainedComparator<Integer> chained = new ChainedComparator<>(Arrays.asList(Comparator.<Integer>naturalOrder(), Comparator.<Integer>reverseOrder()));

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfNoComparators() {
        new ChainedComparator<>(Collections.emptyList());
    }

    @Test
    public void shouldCompareBigger() {
        assertEquals(1, chained.compare(2, 1));
    }

    @Test
    public void shouldCompareSmaller() {
        assertEquals(-1, chained.compare(1, 2));
    }

    @Test
    public void shouldCompareSame() {
        assertEquals(0, chained.compare(2, 2));
    }
}
