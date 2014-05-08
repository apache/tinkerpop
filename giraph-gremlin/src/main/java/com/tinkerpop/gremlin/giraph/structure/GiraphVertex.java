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
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertex extends Vertex<LongWritable, Text, NullWritable, KryoWritable> {

    private VertexProgram vertexProgram;
    private Graph gremlinGraph;
    private com.tinkerpop.gremlin.structure.Vertex gremlinVertex;
    private GiraphComputerMemory computerMemory = new GiraphComputerMemory(this);

    public GiraphVertex() {
    }

    public GiraphVertex(final com.tinkerpop.gremlin.structure.Vertex gremlinVertex) {
        this.gremlinGraph = TinkerGraph.open();
        this.gremlinVertex = gremlinVertex;
        final com.tinkerpop.gremlin.structure.Vertex vertex = this.gremlinGraph.addVertex(Element.ID, Long.valueOf(this.gremlinVertex.getId().toString()), Element.LABEL, this.gremlinVertex.getLabel());
        this.gremlinVertex.getProperties().forEach((k, v) -> vertex.setProperty(k, v.get()));
        this.gremlinVertex.outE().forEach(edge -> {
            final com.tinkerpop.gremlin.structure.Vertex otherVertex = ElementHelper.getOrAddVertex(this.gremlinGraph, edge.getVertex(Direction.IN).getId(), edge.getVertex(Direction.IN).getLabel());
            final Edge gremlinEdge = vertex.addEdge(edge.getLabel(), otherVertex);
            edge.getProperties().forEach((k, v) -> gremlinEdge.setProperty(k, v.get()));
        });
        this.gremlinVertex.inE().forEach(edge -> {
            final com.tinkerpop.gremlin.structure.Vertex otherVertex = ElementHelper.getOrAddVertex(this.gremlinGraph, edge.getVertex(Direction.OUT).getId(), edge.getVertex(Direction.OUT).getLabel());
            final Edge gremlinEdge = otherVertex.addEdge(edge.getLabel(), vertex);
            edge.getProperties().forEach((k, v) -> gremlinEdge.setProperty(k, v.get()));
        });
        this.initialize(new LongWritable(Long.valueOf(this.gremlinVertex.getId().toString())), this.getTextOfSubGraph(), EmptyOutEdges.instance());
    }

    @Override
    public void setConf(final org.apache.giraph.conf.ImmutableClassesGiraphConfiguration configuration) {
        this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.apacheConfiguration(configuration));
    }

    public com.tinkerpop.gremlin.structure.Vertex getGremlinVertex() {
        return this.gremlinVertex;
    }

    public void compute(final Iterable<KryoWritable> messages) {
        inflateGiraphVertex();
        this.vertexProgram.execute(this.gremlinVertex, new GiraphMessenger(this, messages), this.computerMemory);
    }

    private Text getTextOfSubGraph() {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final KryoWriter writer = KryoWriter.create().build();
            writer.writeGraph(bos, this.gremlinGraph);
            bos.flush();
            bos.close();
            return new Text(bos.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void inflateGiraphVertex() {
        if (null == this.gremlinVertex) {
            try {
                final ByteArrayInputStream bis = new ByteArrayInputStream(this.getValue().getBytes());
                final KryoReader reader = KryoReader.create().build();
                this.gremlinGraph = TinkerGraph.open();
                reader.readGraph(bis, this.gremlinGraph);
                bis.close();
                this.gremlinVertex = this.gremlinGraph.v(this.getId().get());
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    // TODO: Move back to read/writeVertex
}
