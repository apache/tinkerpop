package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.Compare;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class CompareTest {

    @Parameterized.Parameters(name = "{index}: {0}.test({1},{2}) = {3}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {Compare.EQUAL, null, null, true},
                {Compare.EQUAL, null, 1, false},
                {Compare.EQUAL, 1, null, false},
                {Compare.EQUAL, 1, 1, true},
                {Compare.EQUAL, "1", "1", true},
                {Compare.EQUAL, 100, 99, false},
                {Compare.EQUAL, 100, 101, false},
                {Compare.EQUAL, "z", "a", false},
                {Compare.EQUAL, "a", "z", false},
                {Compare.NOT_EQUAL, null, null, false},
                {Compare.NOT_EQUAL, null, 1, true},
                {Compare.NOT_EQUAL, 1, null, true},
                {Compare.NOT_EQUAL, 1, 1, false},
                {Compare.NOT_EQUAL, "1", "1", false},
                {Compare.NOT_EQUAL, 100, 99, true},
                {Compare.NOT_EQUAL, 100, 101, true},
                {Compare.NOT_EQUAL, "z", "a", true},
                {Compare.NOT_EQUAL, "a", "z", true},
                {Compare.GREATER_THAN, null, null, false},
                {Compare.GREATER_THAN, null, 1, false},
                {Compare.GREATER_THAN, 1, null, false},
                {Compare.GREATER_THAN, 1, 1, false},
                {Compare.GREATER_THAN, "1", "1", false},
                {Compare.GREATER_THAN, 100, 99, true},
                {Compare.GREATER_THAN, 100, 101, false},
                {Compare.GREATER_THAN, "z", "a", true},
                {Compare.GREATER_THAN, "a", "z", false},
                {Compare.LESS_THAN, null, null, false},
                {Compare.LESS_THAN, null, 1, false},
                {Compare.LESS_THAN, 1, null, false},
                {Compare.LESS_THAN, 1, 1, false},
                {Compare.LESS_THAN, "1", "1", false},
                {Compare.LESS_THAN, 100, 99, false},
                {Compare.LESS_THAN, 100, 101, true},
                {Compare.LESS_THAN, "z", "a", false},
                {Compare.LESS_THAN, "a", "z", true},
                {Compare.GREATER_THAN_EQUAL, null, null, false},
                {Compare.GREATER_THAN_EQUAL, null, 1, false},
                {Compare.GREATER_THAN_EQUAL, 1, null, false},
                {Compare.GREATER_THAN_EQUAL, 1, 1, true},
                {Compare.GREATER_THAN_EQUAL, "1", "1", true},
                {Compare.GREATER_THAN_EQUAL, 100, 99, true},
                {Compare.GREATER_THAN_EQUAL, 100, 101, false},
                {Compare.GREATER_THAN_EQUAL, "z", "a", true},
                {Compare.GREATER_THAN_EQUAL, "a", "z", false},
                {Compare.LESS_THAN_EQUAL, null, null, false},
                {Compare.LESS_THAN_EQUAL, null, 1, false},
                {Compare.LESS_THAN_EQUAL, 1, null, false},
                {Compare.LESS_THAN_EQUAL, 1, 1, true},
                {Compare.LESS_THAN_EQUAL, "1", "1", true},
                {Compare.LESS_THAN_EQUAL, 100, 99, false},
                {Compare.LESS_THAN_EQUAL, 100, 101, true},
                {Compare.LESS_THAN_EQUAL, "z", "a", false},
                {Compare.LESS_THAN_EQUAL, "a", "z", true}
        });
    }

    @Parameterized.Parameter(value = 0)
    public Compare compare;

    @Parameterized.Parameter(value = 1)
    public Object first;

    @Parameterized.Parameter(value = 2)
    public Object second;

    @Parameterized.Parameter(value = 3)
    public boolean expected;

    @Test
    public void shouldTest() {
        assertEquals(expected, compare.test(first, second));
    }
}
