package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedFactory {

    public static ReferencedVertex detach(final Vertex vertex) {
        return vertex instanceof ReferencedVertex ? (ReferencedVertex) vertex : new ReferencedVertex(vertex);
    }

    public static ReferencedEdge detach(final Edge edge) {
        return edge instanceof ReferencedEdge ? (ReferencedEdge) edge : new ReferencedEdge(edge);
    }

    public static ReferencedMetaProperty detach(final MetaProperty metaProperty) {
        return metaProperty instanceof ReferencedMetaProperty ? (ReferencedMetaProperty) metaProperty : new ReferencedMetaProperty(metaProperty);
    }

    public static ReferencedProperty detach(final Property property) {
        return property instanceof ReferencedProperty ? (ReferencedProperty) property : new ReferencedProperty(property);
    }

    public static ReferencedPath detach(final Path path) {
        return path instanceof ReferencedPath ? (ReferencedPath) path : new ReferencedPath(path);
    }

    public static ReferencedElement detach(final Element element) {
        if (element instanceof Vertex)
            return detach((Vertex) element);
        else if (element instanceof Edge)
            return detach((Edge) element);
        else if (element instanceof MetaProperty)
            return detach((MetaProperty) element);
        else
            throw new IllegalArgumentException("The provided argument is an unknown element: " + element);
    }

    /////////////////////////////

    public static Vertex attach(final ReferencedVertex vertex, final Vertex hostVertex) {
        return vertex.attach(hostVertex);
    }

    public static Edge attach(final ReferencedEdge edge, final Vertex hostVertex) {
        return edge.attach(hostVertex);
    }

    public static MetaProperty attach(final ReferencedMetaProperty metaProperty, final Vertex hostVertex) {
        return metaProperty.attach(hostVertex);
    }

    public static Property attach(final ReferencedProperty property, final Vertex hostVertex) {
        return property.attach(hostVertex);
    }

    public static Path attach(final ReferencedPath path, final Vertex hostVertex) {
        return path.attach(hostVertex);
    }

    public static Element attach(final ReferencedElement element, final Vertex hostVertex) {
        if (element instanceof Vertex)
            return attach((ReferencedVertex) element, hostVertex);
        else if (element instanceof Edge)
            return attach((ReferencedEdge) element, hostVertex);
        else if (element instanceof MetaProperty)
            return attach((ReferencedMetaProperty) element, hostVertex);
        else
            throw new IllegalArgumentException("The provided argument is an unknown element: " + element);
    }

    public static Vertex attach(final ReferencedVertex vertex, final Graph hostGraph) {
        return vertex.attach(hostGraph);
    }

    public static Edge attach(final ReferencedEdge edge, final Graph hostGraph) {
        return edge.attach(hostGraph);
    }

    public static MetaProperty attach(final ReferencedMetaProperty metaProperty, final Graph hostGraph) {
        return metaProperty.attach(hostGraph);
    }

    public static Property attach(final ReferencedProperty property, final Graph hostGraph) {
        return property.attach(hostGraph);
    }

    public static Path attach(final ReferencedPath path, final Graph hostGraph) {
        return path.attach(hostGraph);
    }

    public static Element attach(final ReferencedElement element, final Graph hostGraph) {
        if (element instanceof Vertex)
            return attach((ReferencedVertex) element, hostGraph);
        else if (element instanceof Edge)
            return attach((ReferencedEdge) element, hostGraph);
        else if (element instanceof MetaProperty)
            return attach((ReferencedMetaProperty) element, hostGraph);
        else
            throw new IllegalArgumentException("The provided argument is an unknown element: " + element);
    }
}
