package com.tinkerpop.blueprints.computer;

import java.util.concurrent.Future;

/**
 * The GraphComputer is responsible for the execution of a VertexProgram against the vertices in the Graph.
 * A GraphComputer maintains a VertexMemory (local vertex memory) and GraphMemory (global graph memory).
 * It is up to the GraphComputer implementation to determine the appropriate memory structures given the computing substrate.
 * All GraphComputers also maintains levels of memory isolation: Bulk Synchronous Parallel and Dirty Bulk Synchronous Parallel.
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
         * Computations are carried out in a bulk synchronous manner.
         * The results of a vertex property update are visible before the end of the round.
         */
        DIRTY_BSP
    }

    public GraphComputer isolation(Isolation isolation);

    public GraphComputer program(VertexProgram program);

    public Future<ComputeResult> submit();

    public static GraphComputer.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features {
        public default boolean supportsGlobalMessageTypes() {
            return true;
        }

        public default boolean supportsLocalMessageTypes() {
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

        public default boolean supportsVertexPropertyUpdates() {
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

        public default boolean supportsEdgePropertyUpdates() {
            return true;
        }

        public default boolean supportsIsolation(final Isolation isolation) {
            return true;
        }

        public static IllegalStateException adjacentVertexPropertiesCanNotBeRead() {
            return new IllegalStateException("The properties of an adjacent vertex can not be read, only its id");
        }

        public static IllegalStateException adjacentVertexPropertiesCanNotBeWritten() {
            return new IllegalStateException("The properties of an adjacent vertex can not be written");
        }

        public static IllegalArgumentException providedKeyIsNotAComputeKey(final String key) {
            return new IllegalArgumentException("The provided key is not a compute key: " + key);
        }
    }

}
