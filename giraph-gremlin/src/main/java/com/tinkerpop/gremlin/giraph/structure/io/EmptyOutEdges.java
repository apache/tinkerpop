package com.tinkerpop.gremlin.giraph.structure.io;

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
    public void initialize(final Iterable<Edge<LongWritable, NullWritable>> edges) {
    }

    @Override
    public void initialize(final int capacity) {
    }

    @Override
    public void initialize() {
    }

    @Override
    public void add(final Edge<LongWritable, NullWritable> edge) {
    }

    @Override
    public void remove(final LongWritable targetVertexId) {
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Iterator<Edge<LongWritable, NullWritable>> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
    }
}
