package com.tinkerpop.blueprints.generator;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Map;

/**
 * Base class for all synthetic network generators.
 * 
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractGenerator {

    private final String label;
    private final EdgeAnnotator edgeAnnotator;
    private final VertexAnnotator vertexAnnotator;

    /**
     * Constructs a new network generator for edges with the given label and annotator.
     *
     * @param label Label for the generated edges
     * @param edgeAnnotator EdgeAnnotator to use for annotating newly generated edges.
     * @param vertexAnnotator VertexAnnotator to use for annotating process vertices.
     */
    public AbstractGenerator(final String label, final EdgeAnnotator edgeAnnotator,
                             final VertexAnnotator vertexAnnotator) {
        if (label==null || label.isEmpty()) throw new IllegalArgumentException("Label cannot be empty");
        if (edgeAnnotator==null) throw new NullPointerException();
        if (vertexAnnotator==null) throw new NullPointerException();
        this.label = label;
        this.edgeAnnotator = edgeAnnotator;
        this.vertexAnnotator = vertexAnnotator;
    }

    /**
     * Constructs a new network generator for edges with the given label and annotator.
     *
     * @param label Label for the generated edges
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
        this(label,EdgeAnnotator.NONE);
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
