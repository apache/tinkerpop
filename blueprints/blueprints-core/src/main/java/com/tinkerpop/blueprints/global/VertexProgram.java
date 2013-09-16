package com.tinkerpop.blueprints.global;

import com.tinkerpop.blueprints.Vertex;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexProgram extends Serializable {

    public enum KeyType {
        VARIABLE,
        CONSTANT
    }

    /**
     * The method is called at the beginning of the computation.
     * The method is global to the GraphComputer and as such, is not called for each vertex.
     *
     * @param graphMemory The global GraphMemory of the GraphComputer
     */
    public void setup(GraphMemory graphMemory);

    /**
     * This method denotes the main body of computation.
     *
     * @param vertex      the vertex to execute the VertexProgram on
     * @param graphMemory the shared state between all vertices in the computation
     */
    public void execute(Vertex vertex, GraphMemory graphMemory);

    /**
     * The method is called at the end of a round to determine if the computation is complete.
     * The method is global to the GraphComputer and as such, is not called for each vertex.
     *
     * @param graphMemory The global GraphMemory of the GraphComputer
     * @return whether or not to halt the computation
     */
    public boolean terminate(GraphMemory graphMemory);

    public Map<String, KeyType> getComputeKeys();

}
