package com.tinkerpop.gremlin;

import junit.framework.TestCase;

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
}
