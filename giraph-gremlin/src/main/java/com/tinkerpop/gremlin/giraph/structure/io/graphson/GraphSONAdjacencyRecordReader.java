package com.tinkerpop.gremlin.giraph.structure.io.graphson;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import com.tinkerpop.gremlin.util.function.QuintFunction;
import com.tinkerpop.gremlin.util.function.TriFunction;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphSONAdjacencyRecordReader extends RecordReader<NullWritable, GiraphVertex> {

    private final LineRecordReader lineRecordReader;
    private final GraphSONReader graphSONReader;
    private GiraphVertex vertex = null;

    public GraphSONAdjacencyRecordReader() {
        this.lineRecordReader = new LineRecordReader();
        this.graphSONReader = GraphSONReader.create().build();
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        //System.out.println("split: " + genericSplit);
        this.lineRecordReader.initialize(genericSplit, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!this.lineRecordReader.nextKeyValue())
            return false;

        TinkerGraph g = TinkerGraph.open();

        TriFunction<Object, String, Object[], Vertex> vertexMaker = (id, label, props) -> createVertex(g, id, label, props);
        QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker = (id, outId, inId, label, props) -> createEdge(g, id, outId, inId, label, props);

        Vertex v;
        try (InputStream in = new ByteArrayInputStream(lineRecordReader.getCurrentValue().getBytes())) {
            v = graphSONReader.readVertex(in, Direction.BOTH, vertexMaker, edgeMaker);
        }

        vertex = new GiraphVertex(g, v);

        return true;
    }

    private Vertex createVertex(final TinkerGraph g,
                                final Object id,
                                final String label,
                                final Object[] props) {

        Object[] newProps = new Object[props.length + 4];
        System.arraycopy(props, 0, newProps, 0, props.length);
        newProps[props.length] = Element.ID;
        newProps[props.length + 1] = id;
        newProps[props.length + 2] = Element.LABEL;
        newProps[props.length + 3] = label;

        return g.addVertex(newProps);
    }

    private Edge createEdge(final TinkerGraph g,
                            final Object id,
                            final Object outId,
                            final Object inId,
                            final String label,
                            final Object[] props) {
        Vertex outV;
        try {
            outV = g.v(outId);
        } catch (FastNoSuchElementException e) {
            outV = null;
        }
        if (null == outV) {
            outV = g.addVertex(Element.ID, outId);
        }

        Vertex inV;
        try {
            inV = g.v(inId);
        } catch (FastNoSuchElementException e) {
            inV = null;
        }
        if (null == inV) {
            inV = g.addVertex(Element.ID, inId);
        }

        Object[] newProps = new Object[props.length + 2];
        System.arraycopy(props, 0, newProps, 0, props.length);
        newProps[props.length] = Element.ID;
        newProps[props.length + 1] = id;

        return outV.addEdge(label, inV, newProps);
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public GiraphVertex getCurrentValue() {
        return this.vertex;
    }

    @Override
    public float getProgress() throws IOException {
        return this.lineRecordReader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.lineRecordReader.close();
    }
}
