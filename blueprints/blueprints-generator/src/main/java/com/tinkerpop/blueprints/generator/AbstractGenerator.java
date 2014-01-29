package com.tinkerpop.blueprints.generator;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Base class for all synthetic network generators.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGenerator {

    private final String label;
    private final EdgeAnnotator edgeAnnotator;
    private final VertexAnnotator vertexAnnotator;
    protected final Supplier<Long> seedSupplier;

    /**
     * Constructs a new network generator for edges with the given label and annotator.
     *
     * @param label           Label for the generated edges
     * @param edgeAnnotator   EdgeAnnotator to use for annotating newly generated edges.
     * @param vertexAnnotator VertexAnnotator to use for annotating process vertices.
     * @param seedGenerator A {@link Supplier} function to provide seeds to {@link java.util.Random}
     */
    public AbstractGenerator(final String label, final EdgeAnnotator edgeAnnotator,
                             final VertexAnnotator vertexAnnotator, final Optional<Supplier<Long>> seedGenerator) {
        if (label == null || label.isEmpty()) throw new IllegalArgumentException("Label cannot be empty");
        if (edgeAnnotator == null) throw new NullPointerException();
        if (vertexAnnotator == null) throw new NullPointerException();
        if (seedGenerator == null) throw new NullPointerException();
        this.label = label;
        this.edgeAnnotator = edgeAnnotator;
        this.vertexAnnotator = vertexAnnotator;
        this.seedSupplier = seedGenerator.orElse(System::currentTimeMillis);
    }

    /**
     * Constructs a new network generator for edges with the given label and annotator.
     *
     * @param label           Label for the generated edges
     * @param edgeAnnotator   EdgeAnnotator to use for annotating newly generated edges.
     * @param vertexAnnotator VertexAnnotator to use for annotating process vertices.
     */
    public AbstractGenerator(final String label, final EdgeAnnotator edgeAnnotator,
                             final VertexAnnotator vertexAnnotator) {
        this(label, edgeAnnotator, vertexAnnotator, Optional.empty());
    }

    /**
     * Constructs a new network generator for edges with the given label and annotator.
     *
     * @param label     Label for the generated edges
     * @param annotator EdgeAnnotator to use for annotating newly generated edges.
     */
    public AbstractGenerator(final String label, final EdgeAnnotator annotator) {
        this(label, annotator, VertexAnnotator.NONE);
    }

    /**
     * Constructs a new network generator for edges with the given label and an empty annotator.
     *
     * @param label Label for the generated edges
     */
    public AbstractGenerator(final String label) {
        this(label, EdgeAnnotator.NONE);
    }

    /**
     * Returns the label for this generator.
     */
    public final String getLabel() {
        return label;
    }

    /**
     * Returns the {@link EdgeAnnotator} for this generator
     */
    public final EdgeAnnotator getEdgeAnnotator() {
        return edgeAnnotator;
    }

    /**
     * Returns the {@link VertexAnnotator} for this generator
     */
    public final VertexAnnotator getVertexAnnotator() {
        return vertexAnnotator;
    }

    protected final Edge addEdge(final Vertex out, final Vertex in) {
        final Edge e = out.addEdge(label, in);
        edgeAnnotator.annotate(e);
        return e;
    }

    protected final Vertex processVertex(final Vertex vertex, final Map<String, Object> context) {
        vertexAnnotator.annotate(vertex, context);
        return vertex;
    }

}
