package com.tinkerpop.gremlin.util.function;

import org.junit.Test;

import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HexFunctionTest {
    @Test
    public void shouldApplyCurrentFunctionAndThenAnotherSuppliedOne() {
        final HexFunction<String, String, String, String, String, String, String> f = (a,b,c,d,e,g) -> a + b + c + d + e + g;
        final UnaryOperator<String> after = (s) -> s + "last";
        assertEquals("123456last", f.andThen(after).apply("1", "2", "3", "4", "5", "6"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfAfterFunctionIsNull() {
        final HexFunction<String, String, String, String, String, String, String> f = (a,b,c,d,e,g) -> a + b + c + e + g;
        f.andThen(null);
    }
}
