package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphEdge implements Edge<LongWritable, MapWritable> {

    final com.tinkerpop.gremlin.structure.Edge gremlinEdge;
    private final MapWritable properties;
    final LongWritable inVertexId;

    public GiraphEdge(final com.tinkerpop.gremlin.structure.Edge gremlinEdge) {
        this.gremlinEdge = gremlinEdge;
        this.inVertexId = new LongWritable(new Long(this.gremlinEdge.getVertex(Direction.IN).getId().toString()));
        this.properties = new MapWritable();
        this.properties.put(new Text(Element.LABEL), new Text(gremlinEdge.getLabel()));
        gremlinEdge.getProperties().forEach((k, v) -> this.properties.put(new Text(k), new Text(v.toString())));
    }

    public MapWritable getValue() {
        return this.properties;
    }

    public LongWritable getTargetVertexId() {
        return this.inVertexId;
    }

    public com.tinkerpop.gremlin.structure.Edge getGremlinEdge() {
        return this.gremlinEdge;
    }

    public static Iterable<Edge<LongWritable, MapWritable>> createOutEdges(final com.tinkerpop.gremlin.structure.Vertex gremlinVertex) {
        return new OutEdges<LongWritable, MapWritable>() {
            @Override
            public void initialize(Iterable<Edge<LongWritable, MapWritable>> edges) {

            }

            @Override
            public void initialize(int capacity) {

            }

            @Override
            public void initialize() {

            }

            @Override
            public void add(Edge<LongWritable, MapWritable> edge) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void remove(LongWritable targetVertexId) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public int size() {
                return (int) gremlinVertex.out().count();
            }

            @Override
            public Iterator<Edge<LongWritable, MapWritable>> iterator() {
                return (Iterator) gremlinVertex.outE().map(e -> new GiraphEdge(e.get()));
            }

            @Override
            public void write(DataOutput dataOutput) throws IOException {

            }

            @Override
            public void readFields(DataInput dataInput) throws IOException {
            }
        };
    }
}
