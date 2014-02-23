package com.tinkerpop.gremlin.process.olap;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link VertexProgram} represents one component of a distributed graph computation. Each applicable vertex
 * (theoretically) maintains a {@link VertexProgram} instance. The collective behavior of all instances yields
 * the computational result.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexProgram<M extends Serializable> extends Serializable {

    public enum KeyType {
        VARIABLE,
        CONSTANT
    }

    /**
     * The method is called at the beginning of the computation. The method is global to the {@link GraphComputer}
     * and as such, is not called for each vertex.
     *
     * @param graphMemory The global GraphMemory of the GraphComputer
     */
    public void setup(final Graph.Memory graphMemory);

    /**
     * This method denotes the main body of computation that is executed on each vertex in the graph.
     *
     * @param vertex      the {@link com.tinkerpop.gremlin.structure.Vertex} to execute the {@link VertexProgram} on
     * @param messenger   the messenger that moves data between vertices
     * @param graphMemory the shared state between all vertices in the computation
     */
    public void execute(final Vertex vertex, final Messenger<M> messenger, final Graph.Memory graphMemory);

    /**
     * The method is called at the end of a round to determine if the computation is complete. The method is global
     * to the {@link GraphComputer} and as such, is not called for each {@link com.tinkerpop.gremlin.structure.Vertex}.
     *
     * @param graphMemory The global {@link Graph.Memory} of the {@link GraphComputer}
     * @return whether or not to halt the computation
     */
    public boolean terminate(final Graph.Memory graphMemory);

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
