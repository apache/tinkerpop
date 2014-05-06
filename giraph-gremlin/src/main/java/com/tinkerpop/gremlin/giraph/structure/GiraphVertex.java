package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.olap.GiraphComputerMemory;
import com.tinkerpop.gremlin.giraph.process.olap.GiraphMessenger;
import com.tinkerpop.gremlin.giraph.process.olap.KryoWritable;
import com.tinkerpop.gremlin.giraph.process.olap.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertex extends Vertex<LongWritable, BytesWritable, NullWritable, KryoWritable> {

    private static final Logger LOGGER = Logger.getLogger(GiraphVertex.class);

    private VertexProgram vertexProgram;
    private com.tinkerpop.gremlin.structure.Vertex gremlinVertex;
    private GiraphComputerMemory computerMemory;
    private final Graph graph = TinkerGraph.open();

    public GiraphVertex() {
        this.computerMemory = new GiraphComputerMemory(this);
    }

    public GiraphVertex(final com.tinkerpop.gremlin.structure.Vertex gremlinVertex) {
        this();
        this.gremlinVertex = gremlinVertex;
        this.initialize(new LongWritable(Long.valueOf(this.gremlinVertex.getId().toString())), this.getBytesWritable(), EmptyOutEdges.instance());
    }

    @Override
    public void setConf(final ImmutableClassesGiraphConfiguration configuration) {
        this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.apacheConfiguration(configuration));
        KryoWritable.tClass = this.vertexProgram.getMessageClass();
    }

    @Override
    public void initialize(final LongWritable id, final BytesWritable value) {
        this.initialize(id, value, EmptyOutEdges.instance());
    }

    @Override
    public void initialize(final LongWritable id, final BytesWritable value, Iterable<org.apache.giraph.edge.Edge<LongWritable, NullWritable>> edges) {
        try {
            final ByteArrayInputStream bis = new ByteArrayInputStream(value.getBytes());
            final KryoReader reader = KryoReader.create().build();
            this.gremlinVertex = reader.readVertex(bis, (id1, label1, properties1) -> {
                final com.tinkerpop.gremlin.structure.Vertex vertex = this.graph.addVertex(Element.ID, id1, Element.LABEL, label1);
                for (int i = 0; i < properties1.length; i = i + 2) {
                    vertex.setProperty((String) properties1[i], properties1[i + 1]);
                }
                return vertex;
            });
            bis.close();
            super.initialize(id, value, edges);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public com.tinkerpop.gremlin.structure.Vertex getGremlinVertex() {
        return this.gremlinVertex;
    }

    public void compute(final Iterable<KryoWritable> messages) {
        this.vertexProgram.execute(this.gremlinVertex, new GiraphMessenger(this, messages), this.computerMemory);
    }

    private BytesWritable getBytesWritable() {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final KryoWriter writer = KryoWriter.create().build();
            writer.writeVertex(bos, this.gremlinVertex, Direction.BOTH);
            bos.flush();
            bos.close();
            return new BytesWritable(bos.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}
