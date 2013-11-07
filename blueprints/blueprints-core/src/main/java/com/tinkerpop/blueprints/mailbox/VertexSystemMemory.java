package com.tinkerpop.blueprints.mailbox;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexSystemMemory extends VertexMemory {

    public boolean isComputeKey(String key);

    public void completeIteration();

    public void setComputeKeys(final Map<String, VertexProgram.KeyType> computeKeys);

    public Map<String, VertexProgram.KeyType> getComputeKeys();

}