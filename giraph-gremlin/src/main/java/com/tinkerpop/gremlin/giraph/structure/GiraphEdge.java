package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.structure.Direction;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphEdge<I extends WritableComparable> extends GiraphElement implements Edge<I, MapWritable> {

    final com.tinkerpop.gremlin.structure.Edge edge;

    public GiraphEdge(final com.tinkerpop.gremlin.structure.Edge edge) {
        this.edge = edge;
    }

    public I getTargetVertexId() {
        return (I) new ObjectWritable(this.edge.getVertex(Direction.IN).getId());
    }

    public MapWritable getValue() {
        return new MapWritable();
    }
}
