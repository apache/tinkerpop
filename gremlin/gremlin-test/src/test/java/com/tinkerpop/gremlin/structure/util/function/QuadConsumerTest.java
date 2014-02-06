package com.tinkerpop.gremlin.structure.util.function;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class QuadConsumerTest {
    @Test
    public void shouldApplyCurrentFunctionAndThenAnotherSuppliedOne() {
        final List<String> l = new ArrayList<>();
        final QuadConsumer<String, String, String, String> f = (a,b,c,d) -> l.add("first");
        final QuadConsumer<String, String, String, String> after = (a,b,c,d) -> l.add("second");

        f.andThen(after).accept("a","b","c","d");

        assertEquals(2, l.size());
        assertEquals("first", l.get(0));
        assertEquals("second", l.get(1));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfAfterFunctionIsNull() {
        final List<String> l = new ArrayList<>();
        final QuadConsumer<String, String, String, String> f = (a,b,c,d) -> l.add("second");
        f.andThen(null);
    }

    @Test
    public void shouldNotApplySecondIfFirstFails() {
        final List<String> l = new ArrayList<>();
        final QuadConsumer<String, String, String, String> f = (a,b,c,d) -> { throw new RuntimeException(); };
        final QuadConsumer<String, String, String, String> after = (a,b,c,d) -> l.add("second");

        try {
            f.andThen(after).accept("a","b","c","d");
            fail("Should have throw an exception");
        } catch (RuntimeException re) {
            assertEquals(0, l.size());
        }
    }
}
