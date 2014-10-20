package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EdgeOtherVertexStep extends MapStep<Edge, Vertex> implements PathConsumer {

    public EdgeOtherVertexStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> {
            final Path path = traverser.path();
            final Vertex vertex = path.get(path.size() - 2);
            return ElementHelper.areEqual(vertex, traverser.get().iterators().vertexIterator(Direction.OUT).next()) ?
                    traverser.get().iterators().vertexIterator(Direction.IN).next() :
                    traverser.get().iterators().vertexIterator(Direction.OUT).next();
        });
    }
}
