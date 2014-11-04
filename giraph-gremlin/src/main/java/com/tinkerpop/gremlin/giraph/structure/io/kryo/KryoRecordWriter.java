package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KryoRecordWriter extends RecordWriter<NullWritable, GiraphComputeVertex> {

    private final DataOutputStream out;
    private final KryoWriter kryoWriter;

    public KryoRecordWriter(final DataOutputStream out) {
        this.out = out;
        this.kryoWriter = KryoWriter.build().create();
    }

    @Override
    public void write(final NullWritable key, final GiraphComputeVertex vertex) throws IOException {
        if (null != vertex) {
            kryoWriter.writeVertex(out, vertex.getBaseVertex(), Direction.BOTH);
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
