package com.tinkerpop.gremlin.giraph.structure;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertex<I extends WritableComparable, M extends Writable> extends Vertex<I, MapWritable, GiraphEdge, M> {

    public void compute(final Iterable<M> messages) {
        this.getEdges();
    }

}
