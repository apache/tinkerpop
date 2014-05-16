package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.giraph.process.olap.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalCounters;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.io.kryo.GremlinKryo;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GremlinKryoVertexWriter extends VertexWriter {

    private OutputStream outputStream;

    private KryoWriter writer;

    public GremlinKryoVertexWriter() {
       System.out.println("create GremlinKryoVertexWriter");
    }

    public void initialize(final TaskAttemptContext context) throws IOException {
        System.out.println("initialize GremlinKryoVertexWriter");

        GremlinKryo kryo = GremlinKryo.create()
                .addCustom(
                        SimpleHolder.class,
                        TraversalCounters.class)
                .build();
        writer = KryoWriter.create().custom(kryo).build();

        final FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        this.outputStream = fileSystem.create(new Path(context.getConfiguration().get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION)));
    }

    public void writeVertex(final Vertex giraphVertex) throws IOException {
        System.out.println("writing vertex in GremlinKryoVertexWriter: " + giraphVertex);
        this.writer.writeVertex(this.outputStream, ((GiraphVertex) giraphVertex).getGremlinVertex(), Direction.BOTH);
    }

    public void close(final TaskAttemptContext context) throws IOException {
        System.out.println("closing GremlinKryoVertexWriter");
        this.outputStream.flush();
        this.outputStream.close();
    }

    public void setConf(final ImmutableClassesGiraphConfiguration configuration) {

    }
}
