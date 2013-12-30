package com.tinkerpop.blueprints.computer;

/**
 * A ComputeResult provides access to the global graph memory and the local vertex memories.
 * These memory structures provide access to all results of the graph computation.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ComputeResult {

    public GraphMemory getGraphMemory();

    public VertexMemory getVertexMemory();
}
