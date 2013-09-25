package com.tinkerpop.blueprints.computer.util;

import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.computer.Isolation;
import com.tinkerpop.blueprints.computer.VertexProgram;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DefaultGraphComputer implements GraphComputer {

    protected VertexProgram vertexProgram;
    protected Isolation isolation = Isolation.BSP;

    public GraphComputer isolation(final Isolation isolation) {
        this.isolation = isolation;
        return this;
    }

    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }
}
