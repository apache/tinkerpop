package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Pair;

import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class BadGraphComputer implements GraphComputer {
    public GraphComputer isolation(final Isolation isolation) {
        return null;
    }

    public GraphComputer program(final VertexProgram vertexProgram) {
        return null;
    }

    public GraphComputer mapReduce(final MapReduce mapReduce) {
        return null;
    }

    public Future<ComputerResult> submit() {
        return null;
    }
}