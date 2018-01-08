/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.computer;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link VertexProgram} represents one component of a distributed graph computation. Each vertex in the graph
 * (logically) executes the {@link VertexProgram} instance in parallel. The collective behavior yields
 * the computational result. In practice, a "worker" (i.e. task, thread, etc.) is responsible for executing the
 * VertexProgram against each vertex that it has in its vertex set (a subset of the full graph vertex set).
 * At minimum there is one "worker" for each vertex, though this is impractical in practice and {@link GraphComputer}
 * implementations that leverage such a design are not expected to perform well due to the excess object creation.
 * Any local state/fields in a VertexProgram is static to the vertices within the same worker set.
 * It is not safe to assume that the VertexProgram's "worker" state will remain stable between iterations.
 * Hence, the existence of {@link VertexProgram#workerIterationStart} and {@link VertexProgram#workerIterationEnd}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexProgram<M> extends Cloneable {

    public static final String VERTEX_PROGRAM = "gremlin.vertexProgram";

    /**
     * When it is necessary to store the state of the VertexProgram, this method is called.
     * This is typically required when the VertexProgram needs to be serialized to another machine.
     * Note that what is stored is simply the instance/configuration state, not any processed data.
     * The default implementation provided simply stores the VertexProgram class name for reflective reconstruction.
     * It is typically a good idea to VertexProgram.super.storeState().
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
     * @param graph         the graph that the VertexProgram will run against
     * @param configuration the configuration to load the state of the VertexProgram from.
     */
    @Deprecated
    public default void loadState(final Graph graph, final Configuration configuration) {
        loadState(configuration, graph);
    }

    /**
     * When it is necessary to load the state of the VertexProgram, this method is called.
     * This is typically required when the VertexProgram needs to be serialized to another machine.
     * Note that what is loaded is simply the instance state, not any processed data.
     *
     * @param graphs        the graph that the VertexProgram will run against
     * @param configuration the configuration to load the state of the VertexProgram from.
     */
    public default void loadState(final Configuration configuration, final Graph... graphs) {

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
     * This method is called at the start of each iteration of each "computational chunk."
     * The set of vertices in the graph are typically not processed with full parallelism.
     * The vertex set is split into subsets and a worker is assigned to call the {@link VertexProgram#execute} method.
     * The default implementation is a no-op.
     *
     * @param memory The memory at the start of the iteration.
     */
    public default void workerIterationStart(final Memory memory) {

    }

    /**
     * This method is called at the end of each iteration of each "computational chunk."
     * The set of vertices in the graph are typically not processed with full parallelism.
     * The vertex set is split into subsets and a worker is assigned to call the {@link VertexProgram#execute} method.
     * The default implementation is a no-op.
     *
     * @param memory The memory at the end of the iteration.
     */
    public default void workerIterationEnd(final Memory memory) {

    }

    /**
     * The {@link org.apache.tinkerpop.gremlin.structure.Element} properties that will be mutated during the computation.
     * All properties in the graph are readable, but only the keys specified here are writable.
     * The default is an empty set.
     *
     * @return the set of element keys that will be mutated during the vertex program's execution
     */
    public default Set<VertexComputeKey> getVertexComputeKeys() {
        return Collections.emptySet();
    }

    /**
     * The {@link Memory} keys that will be used during the computation.
     * These are the only keys that can be read or written throughout the life of the {@link GraphComputer}.
     * The default is an empty set.
     *
     * @return the set of memory keys that will be read/written
     */
    public default Set<MemoryComputeKey> getMemoryComputeKeys() {
        return Collections.emptySet();
    }

    /**
     * Combine the messages in route to a particular vertex. Useful to reduce the amount of data transmitted over the wire.
     * For example, instead of sending two objects that will ultimately be merged at the vertex destination, merge/combine into one and send that object.
     * If no message combiner is provider, then no messages will be combined.
     * Furthermore, it is not guaranteed the all messages in route to the vertex will be combined and thus, combiner-state should not be used.
     * The result of the vertex program algorithm should be the same regardless of whether message combining is executed or not.
     *
     * @return A optional denoting whether or not their is a message combine associated with the vertex program.
     */
    public default Optional<MessageCombiner<M>> getMessageCombiner() {
        return Optional.empty();
    }

    /**
     * This method returns all the {@link MessageScope} possibilities for a particular iteration of the vertex program.
     * The returned messages scopes are the scopes that will be used to send messages during the stated iteration.
     * It is not a requirement that all stated messages scopes be used, just that it is possible that they be used during the iteration.
     *
     * @param memory an immutable form of the {@link Memory}
     * @return all possible message scopes during said vertex program iteration
     */
    public Set<MessageScope> getMessageScopes(final Memory memory);

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
     * The traverser requirements that are needed when this VP is used as part of a traversal.
     * The default is an empty set.
     *
     * @return the traverser requirements
     */
    public default Set<TraverserRequirement> getTraverserRequirements() {
        return Collections.emptySet();
    }

    /**
     * When multiple workers on a single machine need VertexProgram instances, it is possible to use clone.
     * This will provide a speedier way of generating instances, over the {@link VertexProgram#storeState} and {@link VertexProgram#loadState} model.
     * The default implementation simply returns the object as it assumes that the VertexProgram instance is a stateless singleton.
     *
     * @return A clone of the VertexProgram object
     */
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public VertexProgram<M> clone();

    public GraphComputer.ResultGraph getPreferredResultGraph();

    public GraphComputer.Persist getPreferredPersist();


    /**
     * A helper method to construct a {@link VertexProgram} given the content of the supplied configuration.
     * The class of the VertexProgram is read from the {@link VertexProgram#VERTEX_PROGRAM} static configuration key.
     * Once the VertexProgram is constructed, {@link VertexProgram#loadState} method is called with the provided graph and configuration.
     *
     * @param graph         The graph that the vertex program will execute against
     * @param configuration A configuration with requisite information to build a vertex program
     * @param <V>           The vertex program type
     * @return the newly constructed vertex program
     */
    @Deprecated
    public static <V extends VertexProgram> V createVertexProgram(final Graph graph, final Configuration configuration) {
        return createVertexProgram(configuration, graph);
    }

    /**
     * A helper method to construct a {@link VertexProgram} given the content of the supplied configuration.
     * The class of the VertexProgram is read from the {@link VertexProgram#VERTEX_PROGRAM} static configuration key.
     * Once the VertexProgram is constructed, {@link VertexProgram#loadState} method is called with the provided graph and configuration.
     *
     * @param configuration A configuration with requisite information to build a vertex program
     * @param graphs        The graphs that the vertex program will execute against
     * @param <V>           The vertex program type
     * @return the newly constructed vertex program
     */
    public static <V extends VertexProgram> V createVertexProgram(final Configuration configuration, final Graph... graphs) {
        try {
            final Class<V> vertexProgramClass = (Class) Class.forName(configuration.getString(VERTEX_PROGRAM));
            final Constructor<V> constructor = vertexProgramClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            final V vertexProgram = constructor.newInstance();
            vertexProgram.loadState(configuration, graphs);
            return vertexProgram;
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public interface Builder {

        /**
         * This method should only be used by the underlying compute engine. For VertexProgram configurations, please
         * use specific fluent methods off the builder.
         */
        public Builder configure(final Object... keyValues);

        public <P extends VertexProgram> P create(final Graph... graphs);

    }

    public default Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features {
        public default boolean requiresGlobalMessageScopes() {
            return false;
        }

        public default boolean requiresLocalMessageScopes() {
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
    }
}
