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
import org.apache.tinkerpop.gremlin.process.Processor;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.ProcessorTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.concurrent.Future;

/**
 * The {@link GraphComputer} is responsible for the execution of a {@link VertexProgram} and then a set of {@link MapReduce} jobs
 * over the vertices in the {@link org.apache.tinkerpop.gremlin.structure.Graph}. It is up to the {@link GraphComputer} implementation to determine the
 * appropriate memory structures given the computing substrate.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface GraphComputer extends Processor {

    public static final String GRAPH_COMPUTER = "gremlin.graphComputer";
    public static final String WORKERS = "gremlin.graphComputer.workers";
    public static final String PERSIST = "gremlin.graphComputer.persist";
    public static final String RESULT = "gremlin.graphComputer.result";
    public static final String VERTICES = "gremlin.graphComputer.vertices";
    public static final String EDGES = "gremlin.graphComputer.edges";

    public enum ResultGraph {
        /**
         * When the computation is complete, the {@link org.apache.tinkerpop.gremlin.structure.Graph} in {@link ComputerResult} is the original graph that spawned the graph computer.
         */
        ORIGINAL,
        /**
         * When the computation is complete, the {@link org.apache.tinkerpop.gremlin.structure.Graph} in {@link ComputerResult} is a new graph cloned from the original graph.
         */
        NEW
    }

    public enum Persist {
        /**
         * Write nothing to the declared {@link ResultGraph}.
         */
        NOTHING,
        /**
         * Write vertex and vertex properties to the {@link ResultGraph}.
         */
        VERTEX_PROPERTIES,
        /**
         * Write vertex, vertex properties, and edges to the {@link ResultGraph}.
         */
        EDGES
    }

    /**
     * Set the {@link ResultGraph} of the computation.
     * If this is not set explicitly by the user, then the {@link VertexProgram} can choose the most efficient result for its intended use.
     * If there is no declared vertex program, then the {@link GraphComputer} defaults to {@link ResultGraph#ORIGINAL}.
     *
     * @param resultGraph the type of graph to be returned by {@link ComputerResult#graph}
     * @return the updated GraphComputer with newly set result graph
     */
    public GraphComputer result(final ResultGraph resultGraph);

    /**
     * Set the {@link Persist} level of the computation.
     * If this is not set explicitly by the user, then the {@link VertexProgram} can choose the most efficient persist for the its intended use.
     * If there is no declared vertex program, then the {@link GraphComputer} defaults to {@link Persist#NOTHING}.
     *
     * @param persist the persistence level of the resultant computation
     * @return the updated GraphComputer with newly set persist
     */
    public GraphComputer persist(final Persist persist);

    /**
     * Set the {@link VertexProgram} to be executed by the {@link GraphComputer}.
     * There can only be one VertexProgram for the GraphComputer.
     *
     * @param vertexProgram the VertexProgram to be executed
     * @return the updated GraphComputer with newly set VertexProgram
     */
    public GraphComputer program(final VertexProgram vertexProgram);

    /**
     * Add a {@link MapReduce} job to the set of MapReduce jobs to be executed by the {@link GraphComputer}.
     * There can be any number of MapReduce jobs.
     *
     * @param mapReduce the MapReduce job to add to the computation
     * @return the updated GraphComputer with newly added MapReduce job
     */
    public GraphComputer mapReduce(final MapReduce mapReduce);

    /**
     * Set the desired number of workers to execute the {@link VertexProgram} and {@link MapReduce} jobs.
     * This is a recommendation to the underlying {@link GraphComputer} implementation and is allowed to deviate accordingly by the implementation.
     *
     * @param workers the number of workers to execute the submission
     * @return the updated GraphComputer with newly set worker count
     */
    public GraphComputer workers(final int workers);

    /**
     * Add a filter that will limit which vertices are loaded from the graph source.
     * The provided {@link Traversal} can only check the vertex, its vertex properties, and the vertex property properties.
     * The loaded graph will only have those vertices that pass through the provided filter.
     *
     * @param vertexFilter the traversal to verify whether or not to load the current vertex
     * @return the updated GraphComputer with newly set vertex filter
     * @throws IllegalArgumentException if the provided traversal attempts to access vertex edges
     */
    public GraphComputer vertices(final Traversal<Vertex, Vertex> vertexFilter) throws IllegalArgumentException;

    /**
     * Add a filter that will limit which edges of the vertices are loaded from the graph source.
     * The provided {@link Traversal} can only check the local star graph of the vertex and thus,
     * can not access properties/labels of the adjacent vertices.
     * The vertices of the loaded graph will only have those edges that pass through the provided filter.
     *
     * @param edgeFilter the traversal that determines which edges are loaded for each vertex
     * @return the updated GraphComputer with newly set edge filter
     * @throws IllegalArgumentException if the provided traversal attempts to access adjacent vertices
     */
    public GraphComputer edges(final Traversal<Vertex, Edge> edgeFilter) throws IllegalArgumentException;

    /**
     * Set an arbitrary configuration key/value for the underlying {@link org.apache.commons.configuration.Configuration} in the {@link GraphComputer}.
     * Typically, the other fluent methods in {@link GraphComputer} should be used to configure the computation.
     * However, for some custom configuration in the underlying engine, this method should be used.
     * Different GraphComputer implementations will have different key/values and thus, parameters placed here are generally not universal to all GraphComputer implementations.
     * The default implementation simply does nothing and returns the {@link GraphComputer} unchanged.
     *
     * @param key   the key of the configuration
     * @param value the value of the configuration
     * @return the updated GraphComputer with newly set key/value configuration
     */
    public default GraphComputer configure(final String key, final Object value) {
        return this;
    }

    /**
     * Get the configuration associated with the {@link GraphComputer}
     *
     * @return the GraphComputer's configuration
     */
    public Configuration configuration();

    /**
     * Returns a {@link VertexProgramStrategy} which enables a {@link Traversal} to execute on {@link GraphComputer}.
     *
     * @return a traversal strategy capable of executing traversals on a GraphComputer
     */
    public default ProcessorTraversalStrategy<GraphComputer> getProcessorTraversalStrategy() {
        return new VertexProgramStrategy(this);
    }

    public static <A extends GraphComputer> A open(final Configuration configuration) {
        try {
            return (A) Class.forName(configuration.getString(GRAPH_COMPUTER)).getMethod("open", Configuration.class).invoke(null, configuration);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Submit the {@link VertexProgram} and the set of {@link MapReduce} jobs for execution by the {@link GraphComputer}.
     *
     * @return a {@link Future} denoting a reference to the computational result
     * @deprecated As of release 3.3.0, replaced by use of {@link GraphComputer#submit(Graph)}.
     */
    @Deprecated
    public Future<ComputerResult> submit();

    /**
     * Submit the configured {@link GraphComputer} to the provided {@link Graph}.
     * That is, execute the {@link VertexProgram} over the {@link Graph}.
     *
     * @param graph the graph to execute the vertex program over
     * @return a {@link Future} denoting a reference to the computational result
     */
    @Override
    public default Future<ComputerResult> submit(final Graph graph) {
        return this.submit();
    }

    public default Features features() {
        return new Features() {
        };
    }

    public interface Features {

        public default int getMaxWorkers() {
            return Integer.MAX_VALUE;
        }

        public default boolean supportsGlobalMessageScopes() {
            return true;
        }

        public default boolean supportsLocalMessageScopes() {
            return true;
        }

        public default boolean supportsVertexAddition() {
            return true;
        }

        public default boolean supportsVertexRemoval() {
            return true;
        }

        public default boolean supportsVertexPropertyAddition() {
            return true;
        }

        public default boolean supportsVertexPropertyRemoval() {
            return true;
        }

        public default boolean supportsEdgeAddition() {
            return true;
        }

        public default boolean supportsEdgeRemoval() {
            return true;
        }

        public default boolean supportsEdgePropertyAddition() {
            return true;
        }

        public default boolean supportsEdgePropertyRemoval() {
            return true;
        }

        public default boolean supportsResultGraphPersistCombination(final ResultGraph resultGraph, final Persist persist) {
            return true;
        }

        public default boolean supportsGraphFilter() {
            return true;
        }

        /**
         * Supports {@link VertexProgram} and {@link MapReduce} parameters to be direct referenced Java objects (no serialization required).
         * This is typically true for single machine graph computer engines. For cluster oriented graph computers, this is typically false.
         */
        public default boolean supportsDirectObjects() {
            return true;
        }
    }

    public static class Exceptions {

        private Exceptions() {
        }

        public static UnsupportedOperationException adjacentVertexLabelsCanNotBeRead() {
            return new UnsupportedOperationException("The label of an adjacent vertex can not be read");
        }

        public static UnsupportedOperationException adjacentVertexPropertiesCanNotBeReadOrUpdated() {
            return new UnsupportedOperationException("The properties of an adjacent vertex can not be read or updated");
        }

        public static UnsupportedOperationException adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated() {
            return new UnsupportedOperationException("The edges and vertices of an adjacent vertex can not be read or updated");
        }

        public static UnsupportedOperationException graphFilterNotSupported() {
            return new UnsupportedOperationException("The computer does not support graph filter");
        }

        public static IllegalArgumentException providedKeyIsNotAnElementComputeKey(final String key) {
            return new IllegalArgumentException("The provided key is not an element compute key: " + key);
        }

        public static IllegalArgumentException providedKeyIsNotAMemoryComputeKey(final String key) {
            return new IllegalArgumentException("The provided key is not a memory compute key: " + key);
        }

        public static IllegalArgumentException resultGraphPersistCombinationNotSupported(final ResultGraph resultGraph, final Persist persist) {
            return new IllegalArgumentException("The computer does not support the following result graph and persist combination: " + resultGraph + ":" + persist);
        }

        public static IllegalStateException computerHasAlreadyBeenSubmittedAVertexProgram() {
            return new IllegalStateException("This computer has already had a vertex program submitted to it");
        }

        public static IllegalStateException computerHasNoVertexProgramNorMapReducers() {
            return new IllegalStateException("The computer has no vertex program or map reducers to execute");
        }

        public static UnsupportedOperationException incidentAndAdjacentElementsCanNotBeAccessedInMapReduce() {
            return new UnsupportedOperationException("The computer is in MapReduce mode and a vertex's incident and adjacent elements can not be accessed");
        }

        public static UnsupportedOperationException vertexPropertiesCanNotBeUpdatedInMapReduce() {
            return new UnsupportedOperationException("The computer is in MapReduce mode and a vertex's properties can not be updated");
        }

        public static IllegalArgumentException computerRequiresMoreWorkersThanSupported(final int workers, final int maxWorkers) {
            return new IllegalArgumentException("The computer requires more workers than supported: " + workers + " [max:" + maxWorkers + "]");
        }

        public static IllegalArgumentException vertexFilterAccessesIncidentEdges(final Traversal<Vertex, Vertex> vertexFilter) {
            return new IllegalArgumentException("The provided vertex filter traversal accesses incident edges: " + vertexFilter);
        }

        public static IllegalArgumentException edgeFilterAccessesAdjacentVertices(final Traversal<Vertex, Edge> edgeFilter) {
            return new IllegalArgumentException("The provided edge filter traversal accesses data on adjacent vertices: " + edgeFilter);
        }
    }

}
