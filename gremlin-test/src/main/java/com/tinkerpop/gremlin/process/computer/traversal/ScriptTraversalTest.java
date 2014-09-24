package com.tinkerpop.gremlin.process.computer.traversal;

import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptTraversalTest {

    @Test
    public void shouldRegExCorrectly() {

        final String[] scripts = {"foo.v(123123).out().out()",
                "g.v(\"123)\").out.out",
                "g.v(MyIdGenerator.foo().of(123123)).out.out()",
                "g.v(MyIdGenerator.of(123123)).out().out",
                "g.v(MyIdGenerator.foo().of(123123)).out.out()",
                "g.v(MyIdGenerator.foo().of(123123)).out.out",
        };
        for (final String script : scripts) {
            System.out.println(ScriptTraversal.transformToGlobalScan(script));
        }

    }
}
