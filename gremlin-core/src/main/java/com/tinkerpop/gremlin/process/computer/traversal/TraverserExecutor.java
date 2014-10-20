package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TraverserExecutor {

    public static boolean execute(final Vertex vertex, final Messenger<Traverser.Admin<?>> messenger, final Traversal traversal) {
        throw new UnsupportedOperationException("The execute() method must be implemented by the inheriting class");
    }

    public static Vertex getHostingVertex(final Object object) {
        if (object instanceof Vertex)
            return (Vertex) object;
        else if (object instanceof Edge) {
            return ((Edge) object).iterators().vertexIterator(Direction.OUT).next();
        } else if (object instanceof Property)
            return getHostingVertex(((Property) object).element());
        else
            throw new IllegalStateException("The host of the object is unknown: " + object.toString());
    }

}