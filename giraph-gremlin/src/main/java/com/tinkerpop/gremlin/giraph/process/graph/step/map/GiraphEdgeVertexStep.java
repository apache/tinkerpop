package com.tinkerpop.gremlin.giraph.process.graph.step.map;

import com.tinkerpop.gremlin.giraph.structure.GiraphEdge;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphEdgeVertexStep extends EdgeVertexStep {

    public GiraphEdgeVertexStep(final Traversal traversal, final GiraphGraph graph, final Direction direction) {
        super(traversal, direction);
        this.setFunction(traverser -> {
            final Edge edge = traverser.get();
            return edge instanceof GiraphEdge ?
                    (Iterator) ((GiraphEdge) edge).getTinkerEdge().flatMap(e -> e.get().toV(direction)).map(v -> graph.v(v.get().id())) :
                    (Iterator) edge.toV(direction);
        });
    }
}
