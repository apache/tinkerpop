package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.MapPipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeVertexPipe extends MapPipe<Edge, Vertex> {

    public Direction direction;

    public EdgeVertexPipe(final Pipeline pipeline, final Direction direction) {
        super(pipeline, e -> e.get().getVertex(direction));
        this.direction = direction;
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.direction);
    }
}
