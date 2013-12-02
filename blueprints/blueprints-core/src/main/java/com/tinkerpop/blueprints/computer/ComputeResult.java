package com.tinkerpop.blueprints.computer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ComputeResult {

    public GraphMemory getGraphMemory();

    public VertexMemory getVertexMemory();
}
