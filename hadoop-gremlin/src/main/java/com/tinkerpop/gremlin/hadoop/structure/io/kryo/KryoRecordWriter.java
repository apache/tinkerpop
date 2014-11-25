package com.tinkerpop.gremlin.hadoop.structure.io.kryo;

import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KryoRecordWriter extends RecordWriter<NullWritable, VertexWritable> {

    private final DataOutputStream out;
    private final KryoWriter kryoWriter;

    public KryoRecordWriter(final DataOutputStream out) {
        this.out = out;
        this.kryoWriter = KryoWriter.build().create();
    }

    @Override
    public void write(final NullWritable key, final VertexWritable vertex) throws IOException {
        if (null != vertex) {
            this.kryoWriter.writeVertex(out, vertex.get(), Direction.BOTH);
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        this.out.close();
    }
}
