package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Blueprints;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinMessage implements Serializable {

    public final Object elementId;
    public final Blueprints destination;
    public final String propertyKey;

    private GremlinMessage(final Blueprints destination, final Object elementId, final String propertyKey) {
        this.destination = destination;
        this.elementId = elementId;
        this.propertyKey = propertyKey;

    }

    public static GremlinMessage of(final Object object) {
        Blueprints blueprints = Blueprints.of(object);
        if (blueprints == Blueprints.VERTEX)
            return new GremlinMessage(blueprints, ((Vertex) object).getId(), null);
        else if (blueprints == Blueprints.EDGE)
            return new GremlinMessage(blueprints, ((Edge) object).getId(), null);
        else
            return new GremlinMessage(blueprints, ((Property) object).getElement().getId(), ((Property) object).getKey());
    }
}