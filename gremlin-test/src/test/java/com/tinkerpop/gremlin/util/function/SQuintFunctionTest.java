package com.tinkerpop.gremlin.util.function;

import org.junit.Test;

import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SQuintFunctionTest {
    @Test
    public void shouldApplyCurrentFunctionAndThenAnotherSuppliedOne() {
        final SQuintFunction<String, String, String, String, String, String> f = (a, b, c, d, e) -> a + b + c + d + e;
        final UnaryOperator<String> after = (s) -> s + "last";
        assertEquals("12345last", f.andThen(after).apply("1", "2", "3", "4", "5"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfAfterFunctionIsNull() {
        final SQuintFunction<String, String, String, String, String, String> f = (a, b, c, d, e) -> a + b + c + e;
        f.andThen(null);
    }
}
