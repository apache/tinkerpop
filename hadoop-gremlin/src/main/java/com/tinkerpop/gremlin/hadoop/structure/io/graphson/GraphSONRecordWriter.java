package com.tinkerpop.gremlin.hadoop.structure.io.graphson;

import com.tinkerpop.gremlin.hadoop.structure.hdfs.VertexWritable;
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
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONRecordWriter extends RecordWriter<NullWritable, VertexWritable> {
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
    public void write(final NullWritable key, final VertexWritable vertex) throws IOException {
        if (null != vertex) {
            graphSONWriter.writeVertex(out, vertex.get(), Direction.BOTH);
            out.write(NEWLINE);
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
