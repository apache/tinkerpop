package com.tinkerpop.gremlin.util.function;

import org.junit.Test;

import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TriFunctionTest {
    @Test
    public void shouldApplyCurrentFunctionAndThenAnotherSuppliedOne() {
        final TriFunction<String, String, String, String> f = (a, b, c) -> a + b + c;
        final UnaryOperator<String> after = (s) -> s + "last";
        assertEquals("123last", f.andThen(after).apply("1", "2", "3"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfAfterFunctionIsNull() {
        final TriFunction<String, String, String, String> f = (a, b, c) -> a + b + c;
        f.andThen(null);
    }
}
