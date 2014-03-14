package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

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
public abstract class AbstractGenerator implements Generator {
    protected final Graph g;
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
    AbstractGenerator(final Graph g, final String label, final Optional<Consumer<Edge>> edgeAnnotator,
                      final Optional<BiConsumer<Vertex,Map<String,Object>>> vertexAnnotator,
                      final Supplier<Long> seedGenerator) {
        this.g = g;
        this.label = label;
        this.edgeAnnotator = edgeAnnotator;
        this.vertexAnnotator = vertexAnnotator;
        this.seedSupplier = seedGenerator;
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

    public abstract static class AbstractGeneratorBuilder<T extends AbstractGeneratorBuilder> {
        protected String label;
        protected Optional<Consumer<Edge>> edgeProcessor = Optional.empty();
        protected Optional<BiConsumer<Vertex,Map<String,Object>>> vertexProcessor = Optional.empty();
        protected Supplier<Long> seedSupplier = System::currentTimeMillis;

        public T label(final String label) {
            if (null == label || label.isEmpty()) throw new IllegalArgumentException("Label cannot be empty");
            this.label = label;
            return (T) this;
        }

        public T edgeProcessor(final Consumer<Edge> edgeProcessor) {
            this.edgeProcessor = Optional.ofNullable(edgeProcessor);
            return (T) this;
        }

        /**
         * The function supplied here may be called more than once per vertex depending on the implementation.
         */
        public T vertexProcessor(final BiConsumer<Vertex,Map<String,Object>> vertexProcessor) {
            this.vertexProcessor = Optional.ofNullable(vertexProcessor);
            return (T) this;
        }

        public T seedGenerator(final Supplier<Long> seedGenerator) {
            this.seedSupplier = Optional.ofNullable(seedGenerator).orElse(System::currentTimeMillis);
            return (T) this;
        }
    }
}
