package com.tinkerpop.blueprints.computer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ComputeResult {

    public GraphMemory getGraphMemory();

    public VertexMemory getVertexMemory();
}
