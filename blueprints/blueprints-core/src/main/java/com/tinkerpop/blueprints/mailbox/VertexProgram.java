package com.tinkerpop.blueprints.mailbox;

import com.tinkerpop.blueprints.Vertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexProgram<M extends Serializable> {

    public enum KeyType {
        VARIABLE,
        CONSTANT
    }

    public void setup(GraphMemory graphMemory);

    public void execute(Vertex vertex, Mailbox<M> mailbox, GraphMemory graphMemory);

    public boolean terminate(GraphMemory graphMemory);

    public Map<String, KeyType> getComputeKeys();

    public static Map<String, KeyType> ofComputeKeys(final Object... computeKeys) {
        if (computeKeys.length % 2 != 0)
            throw new IllegalArgumentException("The provided arguments must have a size that is a factor of 2");
        final Map<String, KeyType> keys = new HashMap<>();
        for (int i = 0; i < computeKeys.length; i = i + 2) {
            keys.put(Objects.requireNonNull(computeKeys[i].toString()), (KeyType) Objects.requireNonNull(computeKeys[i + 1]));
        }
        return keys;
    }
}
