package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.util.function.TriFunction;
import org.junit.Test;

import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TriFunctionTest {
    @Test
    public void shouldApplyCurrentFunctionAndThenAnotherSuppliedOne() {
        final TriFunction<String, String, String, String> f = (a,b,c) -> a + b + c;
        final UnaryOperator<String> after = (s) -> s + "last";
        assertEquals("123last", f.andThen(after).apply("1", "2", "3"));
    }
}
