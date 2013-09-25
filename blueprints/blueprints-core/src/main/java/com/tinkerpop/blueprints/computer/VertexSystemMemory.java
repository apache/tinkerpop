package com.tinkerpop.blueprints.computer;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexSystemMemory extends VertexMemory {

    public boolean isComputeKey(String key);

    public void completeIteration();

    public void setComputeKeys(final Map<String, VertexProgram.KeyType> computeKeys);

}