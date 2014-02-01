package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeVertexPipe extends FlatMapPipe<Edge, Vertex> {

    public Direction direction;

    public EdgeVertexPipe(final Pipeline pipeline, final Direction direction) {
        super(pipeline);
        this.direction = direction;
        this.setFunction(holder -> {
            final List<Vertex> vertices = new ArrayList<>();
            if (this.direction.equals(Direction.IN) || this.direction.equals(Direction.BOTH)) {
                vertices.add(holder.get().getVertex(Direction.IN));
            }

            if (this.direction.equals(Direction.OUT) || this.direction.equals(Direction.BOTH)) {
                vertices.add(holder.get().getVertex(Direction.OUT));
            }
            return vertices.iterator();
        });
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.direction);
    }
}
