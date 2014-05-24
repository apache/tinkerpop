package com.tinkerpop.gremlin.giraph.structure.io.graphson;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphSONAdjacencyVertexWriter extends VertexWriter {

    private final GraphSONAdjacencyOutputFormat outputFormat;
    private RecordWriter<NullWritable, GiraphVertex> recordWriter;

    public GraphSONAdjacencyVertexWriter() {
        outputFormat = new GraphSONAdjacencyOutputFormat();
    }

    public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
        recordWriter = outputFormat.getRecordWriter(context);
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        recordWriter.close(context);
    }

    public void writeVertex(Vertex vertex) throws IOException, InterruptedException {
        recordWriter.write(NullWritable.get(), (GiraphVertex) vertex);
    }
}
