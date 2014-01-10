package com.tinkerpop.blueprints.computer;

import java.util.Map;

/**
 * These methods are not intended to be available to the developer of a {@link VertexProgram}. As such, they are in
 * an extending interface with the parent interface being the typical cast.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexSystemMemory extends VertexMemory {

    public default boolean isComputeKey(final String key) {
        return this.getComputeKeys().containsKey(key);
    }

    public default boolean isConstantKey(final String key) {
        return VertexProgram.KeyType.CONSTANT.equals(this.getComputeKeys().get(key));
    }

    public default boolean isVariableKey(final String key) {
        return VertexProgram.KeyType.VARIABLE.equals(this.getComputeKeys().get(key));
    }

    public void completeIteration();

    public void setComputeKeys(final Map<String, VertexProgram.KeyType> computeKeys);

    public Map<String, VertexProgram.KeyType> getComputeKeys();

}