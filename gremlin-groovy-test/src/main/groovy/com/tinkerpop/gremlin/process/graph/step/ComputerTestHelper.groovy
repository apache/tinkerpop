package com.tinkerpop.gremlin.process.graph.step

import com.tinkerpop.gremlin.groovy.engine.GroovyTraversalScript
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.computer.GraphComputer
import com.tinkerpop.gremlin.structure.Graph

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerTestHelper {

    public static final Traversal compute(final String script, final Graph g, final GraphComputer computer) {
        return GroovyTraversalScript.of(script).over(g).using(computer).withSugar().traversal().get();
    }

    public static final Traversal compute(final String script, final Graph g) {
        return GroovyTraversalScript.of(script).over(g).using(g.compute()).withSugar().traversal().get();
    }
}
