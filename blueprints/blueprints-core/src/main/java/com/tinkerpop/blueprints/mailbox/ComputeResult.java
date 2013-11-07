package com.tinkerpop.blueprints.mailbox;

import com.tinkerpop.blueprints.computer.VertexMemory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ComputeResult {

    public com.tinkerpop.blueprints.computer.GraphMemory getGraphMemory();

    public VertexMemory getVertexMemory();
}
