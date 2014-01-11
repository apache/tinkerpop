package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.util.function.QuadFunction;
import org.junit.Test;

import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class QuadFunctionTest {
    @Test
    public void shouldApplyCurrentFunctionAndThenAnotherSuppliedOne() {
        final QuadFunction<String, String, String, String, String> f = (a,b,c,d) -> a + b + c + d;
        final UnaryOperator<String> after = (s) -> s + "last";
        assertEquals("1234last", f.andThen(after).apply("1", "2", "3", "4"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfAfterFunctionIsNull() {
        final QuadFunction<String, String, String, String, String> f = (a,b,c,d) -> a + b + c;
        f.andThen(null);
    }
}
