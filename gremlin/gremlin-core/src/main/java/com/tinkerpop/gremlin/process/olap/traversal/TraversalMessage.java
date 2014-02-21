package com.tinkerpop.gremlin.process.olap.traversal;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.PathHolder;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TraversalMessage implements Serializable {

    protected Holder holder;

    protected TraversalMessage(final Holder holder) {
        this.holder = holder;
        this.holder.deflate();
    }

    public static <T extends TraversalMessage> T of(final Holder holder) {
        if (holder instanceof PathHolder)
            return (T) TraversalPathMessage.of(holder);
        else
            return (T) TraversalCounterMessage.of(holder);
    }

    public static List<Vertex> getHostingVertices(final Object object) {
        if (object instanceof Vertex)
            return Arrays.asList((Vertex) object);
        else if (object instanceof Edge)
            return Arrays.asList(((Edge) object).getVertex(Direction.OUT));
        else if (object instanceof Property)
            return getHostingVertices(((Property) object).getElement());
        else
            throw new IllegalStateException("The host of the object is unknown: " + object.toString());

    }
}