package com.tinkerpop.gremlin;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinTest extends TestCase {

    public void testDoLambda() {
        int y = Gremlin.doLambda(x -> x + 1);
        assertEquals(2, y);
    }
}
