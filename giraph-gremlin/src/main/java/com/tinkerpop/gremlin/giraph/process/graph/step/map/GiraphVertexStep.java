package com.tinkerpop.gremlin.giraph.process.graph.step.map;

import com.tinkerpop.gremlin.giraph.structure.GiraphEdge;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertexStep<E extends Element> extends VertexStep<E> {

    public GiraphVertexStep(final Traversal traversal, final GiraphGraph graph, final Class<E> returnClass, final Direction direction, final int branchFactor, final String... labels) {
        super(traversal, returnClass, direction, branchFactor, labels);
        if (Vertex.class.isAssignableFrom(returnClass))
            this.setFunction(traverser -> {
                final Vertex vertex = traverser.get();
                //return (Iterator) vertex.getBaseVertex().to(direction, branchFactor, labels).map(v -> graph.v(v.get().id()));
                return vertex instanceof GiraphVertex ?
                        (Iterator) ((GiraphVertex) vertex).getBaseVertex().to(direction, branchFactor, labels).map(v -> graph.v(v.get().id())) :
                        (Iterator) traverser.get().to(direction, branchFactor, labels);
            });
        else
            this.setFunction(traverser -> {
                final Vertex vertex = traverser.get();
                //return (Iterator) vertex.getBaseVertex().toE(direction, branchFactor, labels).map(e -> new GiraphEdge((TinkerEdge) e.get(), graph));
                return vertex instanceof GiraphVertex ?
                        (Iterator) ((GiraphVertex) vertex).getBaseVertex().toE(direction, branchFactor, labels).map(e -> new GiraphEdge((TinkerEdge) e.get(), graph)) :
                        (Iterator) traverser.get().toE(direction, branchFactor, labels);
            });
    }
}
