package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GraphStep<E extends Element> extends StartStep<E> implements TraverserSource {

    protected final Class<E> returnClass;

    public GraphStep(final Traversal traversal, final Class<E> returnClass) {
        super(traversal);
        this.returnClass = returnClass;
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, returnClass.getSimpleName().toLowerCase());
    }

    public boolean returnsVertices() {
        return Vertex.class.isAssignableFrom(this.returnClass);
    }

    public boolean returnsEdges() {
        return Edge.class.isAssignableFrom(this.returnClass);
    }
}
