package com.tinkerpop.gremlin.giraph.structure.util;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyOutEdges implements OutEdges<LongWritable, NullWritable> {

    private static final EmptyOutEdges INSTANCE = new EmptyOutEdges();

    public static EmptyOutEdges instance() {
        return INSTANCE;
    }

    @Override
    public void initialize(Iterable<Edge<LongWritable, NullWritable>> edges) {

    }

    @Override
    public void initialize(int capacity) {

    }

    @Override
    public void initialize() {

    }

    @Override
    public void add(Edge<LongWritable, NullWritable> edge) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void remove(LongWritable targetVertexId) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public Iterator<Edge<LongWritable, NullWritable>> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    }
}
