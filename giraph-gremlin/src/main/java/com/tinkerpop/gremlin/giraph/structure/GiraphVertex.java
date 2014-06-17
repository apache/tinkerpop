package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputerGlobals;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphMessenger;
import com.tinkerpop.gremlin.giraph.process.computer.KryoWritable;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
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
    private GiraphGraphComputerGlobals globals;

    public GiraphVertex() {
    }

    public GiraphVertex(final com.tinkerpop.gremlin.structure.Vertex gremlinVertex) {
        this.gremlinGraph = TinkerGraph.open();
        this.gremlinVertex = gremlinVertex;
        final com.tinkerpop.gremlin.structure.Vertex vertex = this.gremlinGraph.addVertex(Element.ID, Long.valueOf(this.gremlinVertex.id().toString()), Element.LABEL, this.gremlinVertex.label());
        this.gremlinVertex.properties().forEach((k, v) -> vertex.property(k, v.value()));
        this.gremlinVertex.outE().forEach(edge -> {
            final com.tinkerpop.gremlin.structure.Vertex otherVertex = ElementHelper.getOrAddVertex(this.gremlinGraph, edge.inV().id().next(), edge.inV().label().next());
            final Edge gremlinEdge = vertex.addEdge(edge.label(), otherVertex);
            edge.properties().forEach((k, v) -> gremlinEdge.property(k, v.value()));
        });
        this.gremlinVertex.inE().forEach(edge -> {
            final com.tinkerpop.gremlin.structure.Vertex otherVertex = ElementHelper.getOrAddVertex(this.gremlinGraph, edge.outV().id().next(), edge.outV().label().next());
            final Edge gremlinEdge = otherVertex.addEdge(edge.label(), vertex);
            edge.properties().forEach((k, v) -> gremlinEdge.property(k, v.value()));
        });
        this.initialize(new LongWritable(Long.valueOf(this.gremlinVertex.id().toString())), this.getTextOfSubGraph(), EmptyOutEdges.instance());
    }

    @Override
    public void setConf(final org.apache.giraph.conf.ImmutableClassesGiraphConfiguration configuration) {
        super.setConf(configuration);
        this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(configuration));
        this.globals = new GiraphGraphComputerGlobals(this);
    }

    public com.tinkerpop.gremlin.structure.Vertex getGremlinVertex() {
        return this.gremlinVertex;
    }

    public void compute(final Iterable<KryoWritable> messages) {
        if (null == this.gremlinVertex)
            inflateGiraphVertex();
        this.vertexProgram.execute(this.gremlinVertex, new GiraphMessenger(this, messages), this.globals);
        this.globals.keys().forEach(key -> {
            this.gremlinVertex.property(Property.hidden(key), this.globals.get(key));
        });
        this.gremlinVertex.property(Property.hidden("runtime"), this.globals.getRuntime());
        this.gremlinVertex.property(Property.hidden("iteration"), this.globals.getIteration());
    }

    ///////////////////////////////////////////////

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
