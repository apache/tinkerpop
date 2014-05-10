package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.filter.HasAnnotationStep;
import com.tinkerpop.gremlin.process.graph.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.filter.IntervalStep;
import com.tinkerpop.gremlin.process.graph.map.AnnotatedValueStep;
import com.tinkerpop.gremlin.process.graph.map.AnnotationValueStep;
import com.tinkerpop.gremlin.process.graph.map.AnnotationValuesStep;
import com.tinkerpop.gremlin.process.graph.map.JumpStep;
import com.tinkerpop.gremlin.process.graph.map.PropertyStep;
import com.tinkerpop.gremlin.process.graph.map.StartStep;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * A {@link Vertex} maintains pointers to both a set of incoming and outgoing {@link Edge} objects. The outgoing edges
 * are those edges for  which the {@link Vertex} is the tail. The incoming edges are those edges for which the
 * {@link Vertex} is the head.
 * <p/>
 * Diagrammatically:
 * <pre>
 * ---inEdges---> vertex ---outEdges--->.
 * </pre>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Vertex extends Element {

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues);

    public GraphTraversal<Vertex, Vertex> out(final int branchFactor, final String... labels);

    public GraphTraversal<Vertex, Vertex> in(final int branchFactor, final String... labels);

    public GraphTraversal<Vertex, Vertex> both(final int branchFactor, final String... labels);

    public GraphTraversal<Vertex, Edge> outE(final int branchFactor, final String... labels);

    public GraphTraversal<Vertex, Edge> inE(final int branchFactor, final String... labels);

    public GraphTraversal<Vertex, Edge> bothE(final int branchFactor, final String... labels);

    ///////////////

    public default GraphTraversal<Vertex, Vertex> out(final String... labels) {
        return this.out(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Vertex> in(final String... labels) {
        return this.in(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Vertex> both(final String... labels) {
        return this.both(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Edge> outE(final String... labels) {
        return this.outE(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Edge> inE(final String... labels) {
        return this.inE(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Edge> bothE(final String... labels) {
        return this.bothE(Integer.MAX_VALUE, labels);
    }

    ///////////////

    public default GraphTraversal<Vertex, Vertex> start() {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>();
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public default GraphTraversal<Vertex, Vertex> aggregate(final String variable, final SFunction... preAggregateFunctions) {
        return (GraphTraversal) this.start().aggregate(variable, preAggregateFunctions);
    }

    public default GraphTraversal<Vertex, Vertex> as(final String as) {
        return (GraphTraversal) this.start().as(as);
    }

    public default GraphTraversal<Vertex, Vertex> identity() {
        return (GraphTraversal) this.start().identity();
    }

    public default <E2> GraphTraversal<Vertex, AnnotatedValue<E2>> annotatedValues(final String propertyKey) {
        return (GraphTraversal) this.start().annotatedValues(propertyKey);
    }

    public default <E2> GraphTraversal<Vertex, Property<E2>> property(final String propertyKey) {
        return (GraphTraversal) this.start().property(propertyKey);
    }

    /**
     * public default <E2> GraphTraversal<Vertex, Property<E2>> properties() {
     * return (GraphTraversal) this.start().values;
     * }*
     */

    public default <E2> GraphTraversal<Vertex, E2> value(final String propertyKey) {
        return (GraphTraversal) this.start().value(propertyKey);
    }

    public default GraphTraversal<Vertex, Vertex> with(final Object... variableValues) {
        return (GraphTraversal) this.start().with(variableValues);
    }

    public default GraphTraversal<Vertex, Vertex> sideEffect(final SConsumer<Holder<Vertex>> consumer) {
        return (GraphTraversal) this.start().sideEffect(consumer);
    }

    ///////////////

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> annotation(final String annotationKey) {
        return this.start().annotation(annotationKey);
    }

    // TODO: test
    public default GraphTraversal<Vertex, Map<String, Object>> annotations(final String... annotationKeys) {
        return this.start().annotations(annotationKeys);
    }

    // TODO: test
    public default GraphTraversal<Vertex, Vertex> filter(final SPredicate<Holder<Vertex>> predicate) {
        return this.start().filter(predicate);
    }

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> flatMap(final SFunction<Holder<Vertex>, Iterator<E2>> function) {
        return this.start().flatMap(function);
    }

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> has(final String key) {
        return this.start().has(key);
    }

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> has(final String key, final Object value) {
        return this.start().has(key, value);
    }

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> has(final String key, final T t, final Object value) {
        return this.start().has(key, t, value);
    }

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> has(final String key, final BiPredicate predicate, final Object value) {
        return this.start().has(key, predicate, value);
    }

    // TODO: test
    public default GraphTraversal<Vertex, Element> has(final String propertyKey, final String annotationKey, final BiPredicate biPredicate, final Object annotationValue) {
        return this.start().has(propertyKey, annotationKey, biPredicate, annotationValue);
    }

    // TODO: test
    public default GraphTraversal<Vertex, Element> has(final String propertyKey, final String annotationKey, final T t, final Object annotationValue) {
        return this.start().has(propertyKey, annotationKey, t, annotationValue);
    }

    // TODO: test
    public default GraphTraversal<Vertex, Element> has(final String propertyKey, final String annotationKey, final Object annotationValue) {
        return this.start().has(propertyKey, annotationKey, annotationValue);
    }

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> hasNot(final String key) {
        return this.start().hasNot(key);
    }

    // TODO: intersect

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.start().interval(key, startValue, endValue);
    }

    // TODO: test
    public default GraphTraversal<Vertex, Vertex> jump(final String as) {
        return this.start().jump(as);
    }

    // TODO: test
    public default GraphTraversal<Vertex, Vertex> jump(final String as, final SPredicate<Holder<Vertex>> ifPredicate) {
        return this.start().jump(as, ifPredicate);
    }

    // TODO: test
    public default GraphTraversal<Vertex, Vertex> jump(final String as, final SPredicate<Holder<Vertex>> ifPredicate, final SPredicate<Holder<Vertex>> emitPredicate) {
        return this.start().jump(as, ifPredicate, emitPredicate);
    }

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> map(final SFunction<Holder<Vertex>, E2> function) {
        return this.start().map(function);
    }

    // TODO: test
    public default <E2> GraphTraversal<Vertex, E2> match(final String inAs, final String outAs, final Traversal... traversals) {
        return this.start().match(inAs, outAs, traversals);
    }

    // TODO: pageRank

    // TODO: test
    public default void remove() {
        this.start().remove();
    }

    // TODO: union

    // TODO: test
    public default GraphTraversal<Vertex, Map<String, Object>> values(final String... propertyKeys) {
        return this.start().values(propertyKeys);
    }

    ///////////////

    public static class Exceptions {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }
    }
}
