package com.tinkerpop.tinkergraph.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.VertexStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.tinkergraph.structure.TinkerHelper;
import com.tinkerpop.tinkergraph.structure.TinkerVertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertexStep<E extends Element> extends VertexStep<E> {

    public TinkerVertexStep(final Traversal traversal, final Class<E> returnClass, final Direction direction, final int branchFactor, final String... labels) {
        super(traversal, returnClass, direction, branchFactor, labels);
        if (Vertex.class.isAssignableFrom(returnClass))
            this.setFunction(holder -> (Iterator) StreamFactory.stream(TinkerHelper.getVertices((TinkerVertex) holder.get(), this.direction, this.labels)).limit(this.branchFactor).iterator());
        else
            this.setFunction(holder -> (Iterator) StreamFactory.stream(TinkerHelper.getEdges((TinkerVertex) holder.get(), this.direction, this.labels)).limit(this.branchFactor).iterator());
    }
}
