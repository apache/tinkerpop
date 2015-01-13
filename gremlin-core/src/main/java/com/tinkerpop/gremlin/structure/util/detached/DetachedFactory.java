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
    public static DetachedVertex detach(final Vertex vertex, final boolean withProperties) {
        return vertex instanceof DetachedVertex ? (DetachedVertex) vertex : new DetachedVertex(vertex, withProperties);
    }

    public static DetachedEdge detach(final Edge edge, final boolean withProperties) {
        return edge instanceof DetachedEdge ? (DetachedEdge) edge : new DetachedEdge(edge, withProperties);
    }

    public static <V> DetachedVertexProperty detach(final VertexProperty<V> vertexProperty, final boolean withProperties) {
        return vertexProperty instanceof DetachedVertexProperty ? (DetachedVertexProperty) vertexProperty : new DetachedVertexProperty<>(vertexProperty, withProperties);
    }

    public static <V> DetachedProperty<V> detach(final Property<V> property) {
        return property instanceof DetachedProperty ? (DetachedProperty<V>) property : new DetachedProperty<>(property);
    }

    public static DetachedPath detach(final Path path, final boolean withProperties) {
        return path instanceof DetachedPath ? (DetachedPath) path : new DetachedPath(path, withProperties);
    }

    public static DetachedElement detach(final Element element, final boolean withProperties) {
        if (element instanceof Vertex)
            return detach((Vertex) element, withProperties);
        else if (element instanceof Edge)
            return detach((Edge) element, withProperties);
        else if (element instanceof VertexProperty)
            return detach((VertexProperty) element, withProperties);
        else
            throw new IllegalArgumentException("The provided argument is an unknown element: " + element + ":" + element.getClass());
    }

    public static <D> D detach(final Object object, final boolean withProperties) {
        if (object instanceof Element) {
            return (D) DetachedFactory.detach((Element) object, withProperties);
        } else if (object instanceof Property) {
            return (D) DetachedFactory.detach((Property) object);
        } else if (object instanceof Path) {
            return (D) DetachedFactory.detach((Path) object, withProperties);
        } else {
            return (D) object;
        }
    }
}
