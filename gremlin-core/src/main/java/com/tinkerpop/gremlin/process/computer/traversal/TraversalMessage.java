package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.Traverser;
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

    protected Traverser.System traverser;

    protected TraversalMessage() {
    }

    protected TraversalMessage(final Traverser.System traverser) {
        this.traverser = traverser;
        this.traverser.deflate();
    }

    public Traverser getTraverser() {
        return this.traverser;
    }

    public static <T extends TraversalMessage> T of(final Traverser.System traverser) {
        if (traverser instanceof PathTraverser)
            return (T) TraversalPathMessage.of(traverser);
        else
            return (T) TraversalCounterMessage.of(traverser);
    }

    public static List<Vertex> getHostingVertices(final Object object) {
        if (object instanceof Vertex)
            return Arrays.asList((Vertex) object);
        else if (object instanceof Edge)
            return Arrays.asList(((Edge) object).iterators().vertices(Direction.OUT).next());
            //else if (object instanceof Property)
            //    return getHostingVertices(((Property) object).getElement());
        else if (object instanceof Property)
            return getHostingVertices(((Property) object).getElement());
        else
            throw new IllegalStateException("The host of the object is unknown: " + object.toString());

    }
}