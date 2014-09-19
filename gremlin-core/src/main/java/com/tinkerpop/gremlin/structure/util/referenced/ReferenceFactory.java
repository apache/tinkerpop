package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceFactory {

    public static ReferencedVertex of(final Vertex vertex) {
        return vertex instanceof ReferencedVertex ? (ReferencedVertex) vertex : new ReferencedVertex(vertex);
    }

    public static ReferencedEdge of(final Edge edge) {
        return edge instanceof ReferencedEdge ? (ReferencedEdge) edge : new ReferencedEdge(edge);
    }

    /*public static ReferencedMetaProperty of(final MetaProperty metaProperty) {
        return metaProperty instanceof ReferencedMetaProperty ? (ReferencedMetaProperty) metaProperty : new ReferencedMetaProperty(metaProperty);
    }*/

    public static ReferencedProperty of(final Property property) {
        return property instanceof ReferencedProperty ? (ReferencedProperty) property : new ReferencedProperty(property);
    }
}
