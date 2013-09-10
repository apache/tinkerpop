package com.tinkerpop.gremlin;

import junit.framework.TestCase;

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinTest extends TestCase {

    public void testDoLambda() {
        String y = Gremlin.doLambda((x, z) -> x + z);
        assertEquals("12", y);
        TriFunction<Integer,Integer,Integer,Integer> tri = (a,b,c) -> a+b+c;
        assertEquals(new Integer(6), tri.blah(1,2,3));

    }

    public void testListFilter() {
        assertEquals(2, Arrays.asList(1,2,3).stream().parallel().filter(x -> x >= 2).count());
    }
}
