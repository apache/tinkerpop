package com.tinkerpop.gremlin.giraph.structure.io.graphson;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphSONRecordWriter extends RecordWriter<NullWritable, GiraphComputeVertex> {
    private static final String UTF8 = "UTF-8";
    private static final byte[] NEWLINE;
    private final DataOutputStream out;
    private final GraphSONWriter graphSONWriter;

    static {
        try {
            NEWLINE = "\n".getBytes(UTF8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("Can not find " + UTF8 + " encoding");
        }
    }

    public GraphSONRecordWriter(final DataOutputStream out) {
        this.out = out;
        this.graphSONWriter = GraphSONWriter.build().create();
    }

    @Override
    public void write(final NullWritable key, final GiraphComputeVertex vertex) throws IOException {
        if (null != vertex) {
            Vertex gremlinVertex = vertex.getBaseVertex();
            graphSONWriter.writeVertex(out, gremlinVertex, Direction.BOTH);
            out.write(NEWLINE);
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
