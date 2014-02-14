package com.tinkerpop.tinkergraph.process.steps.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.map.VertexEdgesStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.util.StreamFactory;
import com.tinkerpop.tinkergraph.TinkerHelper;
import com.tinkerpop.tinkergraph.TinkerVertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertexEdgesStep extends VertexEdgesStep {

    public TinkerVertexEdgesStep(final Traversal traversal, final Direction direction, final int branchFactor, final String... labels) {
        super(traversal, direction, branchFactor, labels);
        this.setFunction(holder -> (Iterator) StreamFactory.stream(TinkerHelper.getEdges((TinkerVertex) holder.get(), this.direction, this.labels)).limit(branchFactor).iterator());
    }


}
