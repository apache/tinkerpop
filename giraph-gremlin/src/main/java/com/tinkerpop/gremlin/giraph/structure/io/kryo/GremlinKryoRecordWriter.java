package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalCounters;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.kryo.GremlinKryo;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.gremlin.structure.io.kryo.VertexTerminator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GremlinKryoRecordWriter extends RecordWriter<NullWritable, GiraphVertex> {

    // TODO: why isn't VertexTerminator.INSTANCE.terminal static?
    //private static final byte[] DELIMITER = VertexTerminator.INSTANCE.terminal;

    private final DataOutputStream out;
    private final KryoWriter kryoWriter;

    public GremlinKryoRecordWriter(final DataOutputStream out) {
        this.out = out;

        GremlinKryo kryo = GremlinKryo.create()
                .addCustom(
                        SimpleTraverser.class,
                        TraversalCounters.class)
                .build();

        this.kryoWriter = KryoWriter.create().custom(kryo).build();
    }

    @Override
    public void write(final NullWritable key, final GiraphVertex vertex) throws IOException {
        if (null != vertex) {
            Vertex gremlinVertex = vertex.getGremlinVertex();
            kryoWriter.writeVertex(out, gremlinVertex, Direction.BOTH);
            //out.write(DELIMITER);
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
