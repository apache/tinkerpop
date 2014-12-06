package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedFactory {
    public static DetachedVertex detach(final Vertex vertex, final boolean asReference) {
        return vertex instanceof DetachedVertex ? (DetachedVertex) vertex : new DetachedVertex(vertex, asReference);
    }

    public static DetachedEdge detach(final Edge edge, final boolean asReference) {
        return edge instanceof DetachedEdge ? (DetachedEdge) edge : new DetachedEdge(edge, asReference);
    }

    public static <V> DetachedVertexProperty detach(final VertexProperty<V> vertexProperty, final boolean asReference) {
        return vertexProperty instanceof DetachedVertexProperty ? (DetachedVertexProperty) vertexProperty : new DetachedVertexProperty<>(vertexProperty, asReference);
    }

    public static <V> DetachedProperty<V> detach(final Property<V> property) {
        return property instanceof DetachedProperty ? (DetachedProperty<V>) property : new DetachedProperty<>(property);
    }

    public static DetachedPath detach(final Path path, final boolean asReference) {
        return path instanceof DetachedPath ? (DetachedPath) path : new DetachedPath(path, asReference);
    }

    public static DetachedElement detach(final Element element, final boolean asReference) {
        if (element instanceof Vertex)
            return detach((Vertex) element, asReference);
        else if (element instanceof Edge)
            return detach((Edge) element, asReference);
        else if (element instanceof VertexProperty)
            return detach((VertexProperty) element, asReference);
        else
            throw new IllegalArgumentException("The provided argument is an unknown element: " + element + ":" + element.getClass());
    }
}
