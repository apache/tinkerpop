package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.olap.GiraphComputerMemory;
import com.tinkerpop.gremlin.giraph.process.olap.GiraphMessenger;
import com.tinkerpop.gremlin.giraph.process.olap.KryoWritable;
import com.tinkerpop.gremlin.giraph.process.olap.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
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
    private Graph gremlinGraph;
    private com.tinkerpop.gremlin.structure.Vertex gremlinVertex;
    private GiraphComputerMemory computerMemory;

    public GiraphVertex() {
        this.computerMemory = new GiraphComputerMemory(this);
    }

    public GiraphVertex(final com.tinkerpop.gremlin.structure.Vertex vertex) {
        this();
        this.gremlinGraph = TinkerGraph.open();
        this.gremlinVertex = this.gremlinGraph.addVertex(Element.ID, Long.valueOf(vertex.getId().toString()), Element.LABEL, vertex.getLabel());
        vertex.getProperties().forEach((k, v) -> this.gremlinVertex.setProperty(k, v.get()));
        vertex.outE().forEach(edge -> {
            final com.tinkerpop.gremlin.structure.Vertex otherVertex = ElementHelper.getOrAddVertex(this.gremlinGraph, edge.getVertex(Direction.IN).getId(), edge.getVertex(Direction.IN).getLabel());
            final Edge gremlinEdge = this.gremlinVertex.addEdge(edge.getLabel(), otherVertex);
            edge.getProperties().forEach((k, v) -> gremlinEdge.setProperty(k, v.get()));
        });
        vertex.inE().forEach(edge -> {
            final com.tinkerpop.gremlin.structure.Vertex otherVertex = ElementHelper.getOrAddVertex(this.gremlinGraph, edge.getVertex(Direction.OUT).getId(), edge.getVertex(Direction.OUT).getLabel());
            final Edge gremlinEdge = otherVertex.addEdge(edge.getLabel(), this.gremlinVertex);
            edge.getProperties().forEach((k, v) -> gremlinEdge.setProperty(k, v.get()));
        });
        super.initialize(new LongWritable((long) this.gremlinVertex.getId()), this.getBytesWritable(), EmptyOutEdges.instance());
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
    public void initialize(final LongWritable id, final BytesWritable value, final Iterable<org.apache.giraph.edge.Edge<LongWritable, NullWritable>> edges) {
        try {
            final ByteArrayInputStream bis = new ByteArrayInputStream(value.getBytes());
            final KryoReader reader = KryoReader.create().build();
            this.gremlinGraph = TinkerGraph.open();
            reader.readGraph(bis, this.gremlinGraph);
            bis.close();
            this.gremlinVertex = this.gremlinGraph.v(id.get());
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
            writer.writeGraph(bos, this.gremlinGraph);
            bos.flush();
            bos.close();
            return new BytesWritable(bos.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}
