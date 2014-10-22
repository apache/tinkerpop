package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;

import java.lang.reflect.Constructor;
import java.util.Collections;
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

    public static final String VERTEX_PROGRAM = "gremlin.vertexProgram";

    /**
     * When it is necessary to store the state of the VertexProgram, this method is called.
     * This is typically required when the VertexProgram needs to be serialized to another machine.
     * Note that what is stored is simply the instance/configuration state, not any processed data.
     * The default implementation provided simply stores the VertexProgram class name for reflective reconstruction.
     * It is typically a good idea to super.storeState().
     *
     * @param configuration the configuration to store the state of the VertexProgram in.
     */
    public default void storeState(final Configuration configuration) {
        configuration.setProperty(VERTEX_PROGRAM, this.getClass().getName());
    }

    /**
     * When it is necessary to load the state of the VertexProgram, this method is called.
     * This is typically required when the VertexProgram needs to be serialized to another machine.
     * Note that what is loaded is simply the instance state, not any processed data.
     *
     * @param configuration the configuration to load the state of the VertexProgram from.
     */
    public default void loadState(final Configuration configuration) {

    }

    /**
     * The method is called at the beginning of the computation.
     * The method is global to the {@link GraphComputer} and as such, is not called for each vertex.
     * During this stage, the {@link Memory} should be initialized to to its "start state."
     *
     * @param memory The global memory of the GraphComputer
     */
    public void setup(final Memory memory);

    /**
     * This method denotes the main body of the computation and is executed on each vertex in the graph.
     * This method is logically executed in parallel on all vertices in the graph.
     * When the {@link Memory} is read, it is according to the aggregated state yielded in the previous iteration.
     * When the {@link Memory} is written, the data will be aggregated at the end of the iteration for reading in the next iteration.
     *
     * @param vertex    the {@link Vertex} to execute the {@link VertexProgram} on
     * @param messenger the messenger that moves data between vertices
     * @param memory    the shared state between all vertices in the computation
     */
    public void execute(final Vertex vertex, final Messenger<M> messenger, final Memory memory);

    /**
     * The method is called at the end of each iteration to determine if the computation is complete.
     * The method is global to the {@link GraphComputer} and as such, is not called for each {@link Vertex}.
     * The {@link Memory} maintains the aggregated data from the last execute() iteration.
     *
     * @param memory The global memory of the {@link GraphComputer}
     * @return whether or not to halt the computation
     */
    public boolean terminate(final Memory memory);

    /**
     * The {@link com.tinkerpop.gremlin.structure.Element} properties that will be mutated during the computation.
     * All properties in the graph are readable, but only the keys specified here are writable.
     * The default is an empty set.
     *
     * @return the set of element keys that will be mutated during the vertex program's execution
     */
    public default Set<String> getElementComputeKeys() {
        return Collections.emptySet();
    }

    /**
     * The {@link Memory} keys that will be used during the computation.
     * These are the only keys that can be read or written throughout the life of the {@link GraphComputer}.
     * The default is an empty set.
     *
     * @return the set of memory keys that will be read/written
     */
    public default Set<String> getMemoryComputeKeys() {
        return Collections.emptySet();
    }

    /**
     * DO NOT USE YET.
     */
    public default Optional<MessageCombiner<M>> getMessageCombiner() {
        return Optional.empty();
    }

    /**
     * The set of {@link MapReduce} jobs that are associated with the {@link VertexProgram}.
     * This is not necessarily the exhaustive list over the life of the {@link GraphComputer}.
     * If MapReduce jobs are declared by GraphComputer.mapReduce(), they are not contained in this set.
     * The default is an empty set.
     *
     * @return the set of {@link MapReduce} jobs associated with this {@link VertexProgram}
     */
    public default Set<MapReduce> getMapReducers() {
        return Collections.emptySet();
    }

    /**
     * A helper method to construct a {@link VertexProgram} given the content of the supplied configuration.
     * The class of the VertexProgram is read from the GraphComputer.VERTEX_PROGRAM static configuration key.
     * Once the VertexProgram is constructed, VertexProgram.loadState() method is called with the provided configuration.
     *
     * @param configuration A configuration with requisite information to build a vertex program
     * @param <V>           The vertex program type
     * @return the newly constructed vertex program
     */
    public static <V extends VertexProgram> V createVertexProgram(final Configuration configuration) {
        try {
            final Class<V> vertexProgramClass = (Class) Class.forName(configuration.getString(VERTEX_PROGRAM));
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

        public Builder configure(final Object... keyValues);

        public <P extends VertexProgram> P create();

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
