package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link VertexProgram} represents one component of a distributed graph computation. Each applicable vertex
 * (theoretically) maintains a {@link VertexProgram} instance. The collective behavior of all instances yields
 * the computational result.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexProgram<M> {

    public default void loadState(final Configuration configuration) {

    }

    public default void storeState(final Configuration configuration) {
        configuration.setProperty(GraphComputer.VERTEX_PROGRAM, this.getClass().getName());
    }

    /**
     * The method is called at the beginning of the computation. The method is global to the {@link GraphComputer}
     * and as such, is not called for each vertex.
     *
     * @param memory The global memory of the GraphComputer
     */
    public void setup(final Memory memory);

    /**
     * This method denotes the main body of computation that is executed on each vertex in the graph.
     *
     * @param vertex    the {@link com.tinkerpop.gremlin.structure.Vertex} to execute the {@link VertexProgram} on
     * @param messenger the messenger that moves data between vertices
     * @param memory    the shared state between all vertices in the computation
     */
    public void execute(final Vertex vertex, final Messenger<M> messenger, final Memory memory);

    /**
     * The method is called at the end of a round to determine if the computation is complete. The method is global
     * to the {@link GraphComputer} and as such, is not called for each {@link com.tinkerpop.gremlin.structure.Vertex}.
     *
     * @param memory The global memory of the {@link GraphComputer}
     * @return whether or not to halt the computation
     */
    public boolean terminate(final Memory memory);

    public default Set<String> getElementComputeKeys() {
        return Collections.emptySet();
    }

    public default Set<String> getMemoryComputeKeys() {
        return Collections.emptySet();
    }

    public default Optional<MessageCombiner<M>> getMessageCombiner() {
        return Optional.empty();
    }

    public default List<MapReduce> getMapReducers() {
        return Collections.emptyList();
    }

    public static <V extends VertexProgram> V createVertexProgram(final Configuration configuration) {
        try {
            final Class<V> vertexProgramClass = (Class) Class.forName(configuration.getString(GraphComputer.VERTEX_PROGRAM));
            final Constructor<V> constructor = vertexProgramClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            final V vertexProgram = constructor.newInstance();
            vertexProgram.loadState(configuration);
            return vertexProgram;
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public interface Builder {

        public <P extends VertexProgram> P create();

        public Builder configure(final Object... keyValues);

    }

    public default Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features {
        public default boolean requiresGlobalMessageTypes() {
            return false;
        }

        public default boolean requiresLocalMessageTypes() {
            return false;
        }

        public default boolean requiresVertexAddition() {
            return false;
        }

        public default boolean requiresVertexRemoval() {
            return false;
        }

        public default boolean requiresVertexPropertyAddition() {
            return false;
        }

        public default boolean requiresVertexPropertyRemoval() {
            return false;
        }

        public default boolean requiresEdgeAddition() {
            return false;
        }

        public default boolean requiresEdgeRemoval() {
            return false;
        }

        public default boolean requiresEdgePropertyAddition() {
            return false;
        }

        public default boolean requiresEdgePropertyRemoval() {
            return false;
        }

        public default boolean requiresAdjacentVertexDeepReference() {
            return false;
        }
    }
}
