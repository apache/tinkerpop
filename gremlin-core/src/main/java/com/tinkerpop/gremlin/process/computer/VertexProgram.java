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

    /**
     * The {@link com.tinkerpop.gremlin.structure.Element} properties that will be mutated during the computation.
     * All properties in the graph are readable, but only the keys specified here are writable.
     *
     * @return the set of element keys that will be mutated during the vertex program's execution
     */
    public default Set<String> getElementComputeKeys() {
        return Collections.emptySet();
    }

    /**
     * The {@link Memory} keys that will be used during the computation.
     * These are the only keys that can be read or written throughout the life of the {@link GraphComputer}.
     *
     * @return the set of memory keys that will be read/written
     */
    public default Set<String> getMemoryComputeKeys() {
        return Collections.emptySet();
    }

    public default Optional<MessageCombiner<M>> getMessageCombiner() {
        return Optional.empty();
    }

    /**
     * The set of {@link MapReduce} jobs that are associated with the {@link VertexProgram}.
     * This is not necessarily the exhaustive list over the life of the {@link GraphComputer}.
     * If MapReduce jobs are declared by GraphComputer.mapReduce(), they are not contained in this set.
     *
     * @return the set of {@link MapReduce} jobs associated with this {@link VertexProgram}
     */
    public default Set<MapReduce> getMapReducers() {
        return Collections.emptySet();
    }

    /**
     * A help method to construct a {@link VertexProgram} given the content of the supplied configuration.
     *
     * @param configuration A configuration with requisite information to build a vertex program
     * @param <V>           The vertex program type
     * @return the newly constructed vertex program
     */
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
