package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.Iterator;
import java.util.function.BiPredicate;

/**
 * A {@link Vertex} maintains pointers to both a set of incoming and outgoing {@link Edge} objects. The outgoing edges
 * are those edges for  which the {@link Vertex} is the tail. The incoming edges are those edges for which the
 * {@link Vertex} is the head.
 * <p>
 * Diagrammatically:
 * <pre>
 * ---inEdges---> vertex ---outEdges--->.
 * </pre>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Vertex extends Element {

    public static final String DEFAULT_LABEL = "vertex";

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues);

    /**
     * The following iterators need to be implemented by the vendor as these define how edges are
     * retrieved off of a vertex. All other steps are derivatives of this and thus, defaulted in Vertex.
     */

    public Iterator<Edge> edges(final Direction direction, final int branchFactor, final String... labels);

    public Iterator<Vertex> vertices(final Direction direction, final int branchFactor, final String... labels);

    /**
     * The following steps are Vertex specific and have default implementations based on the vendor implementations above.
     */

    public default GraphTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().to(direction, branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().toE(direction, branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Vertex> to(final Direction direction, final String... labels) {
        return this.to(direction, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Edge> toE(final Direction direction, final String... labels) {
        return this.toE(direction, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Vertex> out(final int branchFactor, final String... labels) {
        return this.to(Direction.OUT, branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Vertex> in(final int branchFactor, final String... labels) {
        return this.to(Direction.IN, branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Vertex> both(final int branchFactor, final String... labels) {
        return this.to(Direction.BOTH, branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Edge> outE(final int branchFactor, final String... labels) {
        return this.toE(Direction.OUT, branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Edge> inE(final int branchFactor, final String... labels) {
        return this.toE(Direction.IN, branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Edge> bothE(final int branchFactor, final String... labels) {
        return this.toE(Direction.BOTH, branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Vertex> out(final String... labels) {
        return this.to(Direction.OUT, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Vertex> in(final String... labels) {
        return this.to(Direction.IN, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Vertex> both(final String... labels) {
        return this.to(Direction.BOTH, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Edge> outE(final String... labels) {
        return this.toE(Direction.OUT, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Edge> inE(final String... labels) {
        return this.toE(Direction.IN, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Edge> bothE(final String... labels) {
        return this.toE(Direction.BOTH, Integer.MAX_VALUE, labels);
    }

    /**
     * The following steps are general to element but repeated here for the sake of ensuring property type casting.
     */

    public default GraphTraversal<Vertex, Vertex> aggregate(final String variable, final SFunction... preAggregateFunctions) {
        return this.start().aggregate(variable, preAggregateFunctions);
    }

    public default GraphTraversal<Vertex, Vertex> as(final String as) {
        return this.start().as(as);
    }

    public default GraphTraversal<Vertex, Vertex> filter(final SPredicate<Traverser<Vertex>> predicate) {
        return this.start().filter(predicate);
    }

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> flatMap(final SFunction<Traverser<Vertex>, Iterator<E2>> function) {
        return this.start().flatMap(function);
    }

    public default <E2> GraphTraversal<Vertex, E2> has(final String key) {
        return this.start().has(key);
    }

    public default <E2> GraphTraversal<Vertex, E2> has(final String key, final Object value) {
        return this.start().has(key, value);
    }

    public default <E2> GraphTraversal<Vertex, E2> has(final String key, final T t, final Object value) {
        return this.start().has(key, t, value);
    }

    public default <E2> GraphTraversal<Vertex, E2> has(final String key, final BiPredicate predicate, final Object value) {
        return this.start().has(key, predicate, value);
    }

    public default <E2> GraphTraversal<Vertex, E2> hasNot(final String key) {
        return this.start().hasNot(key);
    }

    // TODO: intersect

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.start().interval(key, startValue, endValue);
    }

    public default GraphTraversal<Vertex, Vertex> identity() {
        return this.start().identity();
    }

    // TODO: test
    public default GraphTraversal<Vertex, Vertex> jump(final String as) {
        return this.start().jump(as);
    }

    // TODO: test
    public default GraphTraversal<Vertex, Vertex> jump(final String as, final SPredicate<Traverser<Vertex>> ifPredicate) {
        return this.start().jump(as, ifPredicate);
    }

    // TODO: test
    public default GraphTraversal<Vertex, Vertex> jump(final String as, final SPredicate<Traverser<Vertex>> ifPredicate, final SPredicate<Traverser<Vertex>> emitPredicate) {
        return this.start().jump(as, ifPredicate, emitPredicate);
    }

    public default <E2> GraphTraversal<Vertex, E2> map(final SFunction<Traverser<Vertex>, E2> function) {
        return this.start().map(function);
    }

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> match(final String inAs, final String outAs, final Traversal... traversals) {
        return this.start().match(inAs, outAs, traversals);
    }

    // TODO: pageRank


    public default GraphTraversal<Vertex, Vertex> sideEffect(final SConsumer<Traverser<Vertex>> consumer) {
        return this.start().sideEffect(consumer);
    }

    public default GraphTraversal<Vertex, Vertex> start() {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>();
        return (GraphTraversal) traversal.addStep(new StartStep<Vertex>(traversal, this));
    }

    // TODO: union


    public default GraphTraversal<Vertex, Vertex> with(final Object... variableValues) {
        return this.start().with(variableValues);
    }

    ///////////////

    public static class Exceptions {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }

        public static IllegalStateException vertexRemovalNotSupported() {
            return new IllegalStateException("Vertex removal are not supported");
        }

        public static IllegalStateException edgeAdditionsNotSupported() {
            return new IllegalStateException("Edge additions not supported");
        }
    }
}
