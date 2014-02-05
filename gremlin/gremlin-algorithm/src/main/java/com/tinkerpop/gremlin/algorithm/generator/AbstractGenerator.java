package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Base class for all synthetic network generators.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGenerator {

    private final String label;
    private final Optional<Consumer<Edge>> edgeAnnotator;
    private final Optional<BiConsumer<Vertex,Map<String,Object>>> vertexAnnotator;
    protected final Supplier<Long> seedSupplier;

    /**
     * Constructs a new network generator for edges with the given label and annotator. If a {@code seedGenerator} is
     * not supplied then the system clock is used to generate a seed.
     *
     * @param label           Label for the generated edges
     * @param edgeAnnotator   {@link java.util.function.Consumer} to use for annotating newly generated edges.
     * @param vertexAnnotator {@link java.util.function.Consumer} to use for annotating process vertices.
     * @param seedGenerator A {@link java.util.function.Supplier} function to provide seeds to {@link java.util.Random}
     */
    public AbstractGenerator(final String label, final Optional<Consumer<Edge>> edgeAnnotator,
                             final Optional<BiConsumer<Vertex,Map<String,Object>>> vertexAnnotator,
                             final Optional<Supplier<Long>> seedGenerator) {
        if (label == null || label.isEmpty()) throw new IllegalArgumentException("Label cannot be empty");
        if (edgeAnnotator == null) throw new IllegalArgumentException("edgeAnnotator");
        if (vertexAnnotator == null) throw new IllegalArgumentException("vertexAnnotator");
        if (seedGenerator == null) throw new IllegalArgumentException("seedGenerator");
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
    public AbstractGenerator(final String label, final Optional<Consumer<Edge>> edgeAnnotator,
                             final Optional<BiConsumer<Vertex,Map<String,Object>>> vertexAnnotator) {
        this(label, edgeAnnotator, vertexAnnotator, Optional.empty());
    }

    /**
     * Constructs a new network generator for edges with the given label and annotator.
     *
     * @param label     Label for the generated edges
     * @param annotator EdgeAnnotator to use for annotating newly generated edges.
     */
    public AbstractGenerator(final String label, final Optional<Consumer<Edge>> annotator) {
        this(label, annotator, Optional.empty());
    }

    /**
     * Constructs a new network generator for edges with the given label and an empty annotator.
     *
     * @param label Label for the generated edges
     */
    public AbstractGenerator(final String label) {
        this(label, Optional.empty());
    }

    /**
     * Returns the label for this generator.
     */
    public final String getLabel() {
        return label;
    }

    /**
     * Returns the {@link java.util.function.Consumer} for this generator
     */
    @SuppressWarnings("UnusedDeclaration")
    public final Optional<Consumer<Edge>> getEdgeAnnotator() {
        return edgeAnnotator;
    }

    /**
     * Returns the {@link java.util.function.BiConsumer} for this generator
     */
    @SuppressWarnings("UnusedDeclaration")
    public final Optional<BiConsumer<Vertex,Map<String,Object>>> getVertexAnnotator() {
        return vertexAnnotator;
    }

    protected final Edge addEdge(final Vertex out, final Vertex in) {
        final Edge e = out.addEdge(label, in);
        edgeAnnotator.ifPresent(c->c.accept(e));
        return e;
    }

    protected final Vertex processVertex(final Vertex vertex, final Map<String, Object> context) {
        vertexAnnotator.ifPresent(c->c.accept(vertex, context));
        return vertex;
    }

}
