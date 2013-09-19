package com.tinkerpop.gremlin;

import junit.framework.TestCase;

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MonadTest extends TestCase {

    public void testMonad() {

        Monad m = Monad.of("marko").map(x -> x.length())
                .map(x -> x.toString() + "121")
                .map(x -> x.length())
                .map(x -> x.floatValue())
                .filter(x -> x > 0.1)
                .map(x -> x + 134.0d)
                .flatMap(x -> Arrays.asList(x, x, x));

        /*while (m.hasNext()) {
            System.out.println(m.next());
        }*/

    }
}
