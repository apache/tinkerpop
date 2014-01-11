package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.util.function.HexConsumer;
import com.tinkerpop.blueprints.util.function.TriConsumer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HexConsumerTest {
    @Test
    public void shouldApplyCurrentFunctionAndThenAnotherSuppliedOne() {
        final List<String> l = new ArrayList<>();
        final HexConsumer<String, String, String, String, String, String> f = (a,b,c,d,e,g) -> l.add("first");
        final HexConsumer<String, String, String, String, String, String> after = (a,b,c,d,e,g) -> l.add("second");

        f.andThen(after).accept("a","b","c","d","e","f");

        assertEquals(2, l.size());
        assertEquals("first", l.get(0));
        assertEquals("second", l.get(1));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfAfterFunctionIsNull() {
        final List<String> l = new ArrayList<>();
        final HexConsumer<String, String, String, String, String, String> f = (a,b,c,d,e,g) -> l.add("second");
        f.andThen(null);
    }

    @Test
    public void shouldNotApplySecondIfFirstFails() {
        final List<String> l = new ArrayList<>();
        final HexConsumer<String, String, String, String, String, String> f = (a,b,c,d,e,g) -> { throw new RuntimeException(); };
        final HexConsumer<String, String, String, String, String, String> after = (a,b,c,d,e,g) -> l.add("second");

        try {
            f.andThen(after).accept("a","b","c","d","e","f");
            fail("Should have throw an exception");
        } catch (RuntimeException re) {
            assertEquals(0, l.size());
        }
    }
}
