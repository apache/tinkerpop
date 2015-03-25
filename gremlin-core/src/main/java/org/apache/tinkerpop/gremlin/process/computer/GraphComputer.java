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

import java.util.concurrent.Future;

/**
 * The {@link GraphComputer} is responsible for the execution of a {@link VertexProgram} and then a set of {@link MapReduce} jobs
 * over the vertices in the {@link org.apache.tinkerpop.gremlin.structure.Graph}. It is up to the {@link GraphComputer} implementation to determine the
 * appropriate memory structures given the computing substrate. {@link GraphComputer} implementations also
 * maintains levels of memory {@link Isolation}: Bulk Synchronous and Dirty Bulk Synchronous.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface GraphComputer {

    public enum Isolation {
        /**
         * Computations are carried out in a bulk synchronous manner.
         * The results of a vertex property update are only visible after the round is complete.
         */
        BSP,
        /**
         * Computations are carried out in an bulk asynchronous manner.
         * The results of a vertex property update are visible before the end of the round.
         */
        DIRTY_BSP
    }

    public enum ResultGraph {
        /**
         * When the computation is complete, the {@link org.apache.tinkerpop.gremlin.structure.Graph} in {@link ComputerResult} is the original graph that spawned the graph computer.
         */
        ORIGINAL_GRAPH,
        /**
         * When the computation is complete, the {@link org.apache.tinkerpop.gremlin.structure.Graph} in {@link ComputerResult} is a new graph cloned from the original graph.
         */
        NEW_GRAPH
    }

    public enum Persist {
        /**
         * Write nothing to the declared {@link ResultGraph}.
         */
        NOTHING,
        /**
         * Write vertex and vertex properties back to the {@link ResultGraph}.
         */
        VERTEX_PROPERTIES,
        /**
         * Write vertex, vertex properties, and edges back to the {@link ResultGraph}.
         */
        EDGES
    }

    /**
     * Set the {@link Isolation} of the computation.
     *
     * @param isolation the isolation of the computation
     * @return the updated GraphComputer with newly set isolation
     */
    public GraphComputer isolation(final Isolation isolation);

    /**
     * Set the {@link ResultGraph} of the computation.
     * If this is not set explicitly by the user, then the {@link VertexProgram} can choose the most efficient result for its intended use.
     *
     * @param resultGraph the type of graph to be returned by {@link ComputerResult#graph}
     * @return the updated GraphComputer with newly set result graph.
     */
    public GraphComputer result(final ResultGraph resultGraph);

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
     * Submit the {@link VertexProgram} and the set of {@link MapReduce} jobs for execution by the {@link GraphComputer}.
     *
     * @return a {@link Future} denoting a reference to the asynchronous computation and where to get the {@link org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult} when its is complete.
     */
    public Future<ComputerResult> submit();

    public default Features features() {
        return new Features() {
        };
    }

    public interface Features {
        public default boolean supportsWorkerPersistenceBetweenIterations() {
            return true;
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

        public default boolean supportsIsolation(final Isolation isolation) {
            return true;
        }

        public default boolean supportsDirectObjects() {
            return true;
        }
    }

    public static class Exceptions {
        public static IllegalStateException adjacentElementPropertiesCanNotBeRead() {
            return new IllegalStateException("The properties of an adjacent element can not be read, only its id");
        }

        public static IllegalStateException adjacentElementPropertiesCanNotBeWritten() {
            return new IllegalStateException("The properties of an adjacent element can not be written");
        }

        public static IllegalArgumentException providedKeyIsNotAnElementComputeKey(final String key) {
            return new IllegalArgumentException("The provided key is not an element compute key: " + key);
        }

        public static IllegalArgumentException providedKeyIsNotAMemoryComputeKey(final String key) {
            return new IllegalArgumentException("The provided key is not a memory compute key: " + key);
        }

        public static IllegalStateException adjacentVerticesCanNotBeQueried() {
            return new IllegalStateException("It is not possible to query an adjacent vertex in a vertex program");
        }

        public static IllegalArgumentException isolationNotSupported(final Isolation isolation) {
            return new IllegalArgumentException("The provided isolation is not supported by this graph computer: " + isolation);
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

        public static IllegalStateException incidentAndAdjacentElementsCanNotBeAccessedInMapReduce() {
            return new IllegalStateException("The computer is in MapReduce mode and a vertex's incident and adjacent elements can not be accessed");
        }
    }

}
