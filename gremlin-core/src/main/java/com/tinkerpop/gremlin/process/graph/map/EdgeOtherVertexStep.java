package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.PathConsumer;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeOtherVertexStep extends MapStep<Edge, Vertex> implements PathConsumer {

    public EdgeOtherVertexStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(holder -> {
            final Path path = holder.getPath();
            final Vertex vertex = path.get(path.size() - 2);
            return vertex.getId().equals(holder.get().getVertex(Direction.OUT)) ?
                    holder.get().getVertex(Direction.OUT) :
                    holder.get().getVertex(Direction.IN);
        });
    }
}
