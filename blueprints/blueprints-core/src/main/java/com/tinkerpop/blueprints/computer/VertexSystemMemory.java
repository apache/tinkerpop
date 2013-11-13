package com.tinkerpop.blueprints.computer;

import java.util.Map;

/**
 * These methods are not intended to be available to the developer of a VertexProgram.
 * As such, they are in an extending interface with the parent interface being the typical cast.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexSystemMemory extends VertexMemory {

    public boolean isComputeKey(String key);

    public void completeIteration();

    public void setComputeKeys(final Map<String, VertexProgram.KeyType> computeKeys);

    public Map<String, VertexProgram.KeyType> getComputeKeys();

}