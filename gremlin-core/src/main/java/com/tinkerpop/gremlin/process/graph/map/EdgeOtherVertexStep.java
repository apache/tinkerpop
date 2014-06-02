package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.PathConsumer;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
//TODO: Does this implement reversible?
public class EdgeOtherVertexStep extends MapStep<Edge, Vertex> implements PathConsumer {

    public EdgeOtherVertexStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> {
            final Path path = traverser.getPath();
            final Vertex vertex = path.get(path.size() - 2);
            return ElementHelper.areEqual(vertex, traverser.get().outV().next()) ?
                    traverser.get().inV().next() :
                    traverser.get().outV().next();
        });
    }
}
