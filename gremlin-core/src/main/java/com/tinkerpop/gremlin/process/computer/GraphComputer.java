package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * The {@link GraphComputer} is responsible for the execution of a {@link VertexProgram} against the vertices in the
 * Graph. It is up to the {@link GraphComputer} implementation to determine the
 * appropriate memory structures given the computing substrate. {@link GraphComputer} implementations also
 * maintains levels of memory isolation: Bulk Synchronous and Dirty Bulk Synchronous.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface GraphComputer extends TraversalEngine {

    public static String VERTEX_PROGRAM = "gremlin.vertexProgram";

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

    public GraphComputer isolation(final Isolation isolation);

    public GraphComputer program(final Configuration configuration);

    public Future<Pair<Graph, SideEffects>> submit();

    public static void mergeComputedView(final Graph original, final Graph computed, Map<String, String> keyMapping) {
        throw new IllegalStateException("The mergeComputedView method must be defined by the implementing GraphComputer class");
    }

    public interface SideEffects {

        public Set<String> getVariables();

        public <R> R get(final String variable);

        public void set(final String variable, Object value);

        public default Map<String, Object> asMap() {
            final Map<String, Object> map = getVariables().stream()
                    .map(key -> Pair.<String, Object>with(key, get(key)))
                    .collect(Collectors.toMap(kv -> kv.getValue0(), Pair::getValue1));
            return Collections.unmodifiableMap(map);
        }

        public int getIteration();

        public long getRuntime();

        public void setIfAbsent(final String variable, final Object value);

        public long incr(final String variable, final long delta);

        public boolean and(final String variable, final boolean bool);

        public boolean or(final String variable, final boolean bool);

        public default boolean isInitialIteration() {
            return this.getIteration() == 0;
        }

        public interface Administrative extends SideEffects {

            public void incrIteration();

            public void setRuntime(final long runtime);
        }


        public static class Exceptions {

            public static IllegalArgumentException sideEffectVariableCanNotBeEmpty() {
                return new IllegalArgumentException("Graph computer side-effect variables can not be the empty string");
            }

            public static IllegalArgumentException sideEffectVariableCanNotBeNull() {
                return new IllegalArgumentException("Graph computer side-effect variables can not be null");
            }

            public static IllegalArgumentException sideEffectValueCanNotBeNull() {
                return new IllegalArgumentException("Graph computer side-effect value can not be null");
            }

            public static UnsupportedOperationException dataTypeOfSideEffectValueNotSupported(final Object val) {
                return new UnsupportedOperationException(String.format("Graph computer side-effect value [%s] is of type %s is not supported", val, val.getClass()));
            }
        }

    }

    public default Features getFeatures() {
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

        public default boolean supportsAdjacentVertexDeepReference() {
            return true;
        }

        public default boolean supportsIsolation(final Isolation isolation) {
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

        public static IllegalArgumentException providedKeyIsNotAComputeKey(final String key) {
            return new IllegalArgumentException("The provided key is not a compute key: " + key);
        }

        public static IllegalStateException constantComputeKeyHasAlreadyBeenSet(final String key, final Object id) {
            return new IllegalStateException("The constant compute " + key + " has already been set for annotation " + id + ":" + key);
        }

        public static IllegalStateException adjacentVerticesCanNotBeQueried() {
            return new IllegalStateException("It is not possible to query an adjacent vertex in a vertex program");
        }

        public static IllegalArgumentException isolationNotSupported(final Isolation isolation) {
            return new IllegalArgumentException("The provided isolation is not supported by this graph computer: " + isolation);
        }

        public static IllegalStateException computerHasAlreadyBeenSubmittedAVertexProgram() {
            return new IllegalStateException("This computer has already had a vertex program submitted to it");
        }
    }

}
