package com.tinkerpop.gremlin.giraph.structure.io.graphson;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONRecordReader extends RecordReader<NullWritable, GiraphComputeVertex> {

    private final LineRecordReader lineRecordReader;
    private final GraphSONReader graphSONReader;
    private GiraphComputeVertex vertex = null;

    public GraphSONRecordReader() {
        this.lineRecordReader = new LineRecordReader();
        this.graphSONReader = GraphSONReader.build().create();
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        this.lineRecordReader.initialize(genericSplit, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!this.lineRecordReader.nextKeyValue())
            return false;

        final TinkerGraph g = TinkerGraph.open();

        final Function<DetachedVertex, Vertex> vertexMaker = detachedVertex -> DetachedVertex.addTo(g, detachedVertex);
        final Function<DetachedEdge, Edge> edgeMaker = detachedEdge -> DetachedEdge.addTo(g, detachedEdge);

        final TinkerVertex v;
        try (InputStream in = new ByteArrayInputStream(this.lineRecordReader.getCurrentValue().getBytes())) {
            v = (TinkerVertex) this.graphSONReader.readVertex(in, Direction.BOTH, vertexMaker, edgeMaker);
        }

        this.vertex = new GiraphComputeVertex(v);
        return true;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public GiraphComputeVertex getCurrentValue() {
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
