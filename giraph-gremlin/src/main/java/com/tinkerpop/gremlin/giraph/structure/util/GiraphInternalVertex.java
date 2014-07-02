package com.tinkerpop.gremlin.giraph.structure.util;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputerGlobals;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphMessenger;
import com.tinkerpop.gremlin.giraph.process.computer.GlobalsMapReduce;
import com.tinkerpop.gremlin.giraph.process.computer.KryoWritable;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
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
public class GiraphInternalVertex extends Vertex<LongWritable, Text, NullWritable, KryoWritable> {

    //TODO: Dangerous that the underlying TinkerGraph Vertex can have edges written to it.

    private VertexProgram vertexProgram;
    private TinkerGraph tinkerGraph;
    private TinkerVertex tinkerVertex;

    private GiraphGraphComputerGlobals globals;

    public GiraphInternalVertex() {
    }

    public GiraphInternalVertex(final TinkerVertex tinkerVertex) {
        this.tinkerGraph = TinkerGraph.open();
        this.tinkerVertex = tinkerVertex;
        final TinkerVertex vertex = (TinkerVertex) this.tinkerGraph.addVertex(Element.ID, Long.valueOf(this.tinkerVertex.id().toString()), Element.LABEL, this.tinkerVertex.label());
        this.tinkerVertex.properties().forEach((k, v) -> vertex.property(k, v.value()));
        this.tinkerVertex.outE().forEach(edge -> {
            final TinkerVertex otherVertex = (TinkerVertex) ElementHelper.getOrAddVertex(this.tinkerGraph, edge.inV().id().next(), edge.inV().label().next());
            final TinkerEdge gremlinEdge = (TinkerEdge) vertex.addEdge(edge.label(), otherVertex);
            edge.properties().forEach((k, v) -> gremlinEdge.property(k, v.value()));
        });
        this.tinkerVertex.inE().forEach(edge -> {
            final TinkerVertex otherVertex = (TinkerVertex) ElementHelper.getOrAddVertex(this.tinkerGraph, edge.outV().id().next(), edge.outV().label().next());
            final TinkerEdge gremlinEdge = (TinkerEdge) otherVertex.addEdge(edge.label(), vertex);
            edge.properties().forEach((k, v) -> gremlinEdge.property(k, v.value()));
        });
        this.initialize(new LongWritable(Long.valueOf(this.tinkerVertex.id().toString())), this.deflateGiraphVertex(), EmptyOutEdges.instance());
        // TODO? this.tinkerVertex = vertex;
    }

    @Override
    public void setConf(final org.apache.giraph.conf.ImmutableClassesGiraphConfiguration configuration) {
        super.setConf(configuration);
        this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(configuration));
        this.globals = new GiraphGraphComputerGlobals(this);
    }

    public TinkerVertex getTinkerVertex() {
        return this.tinkerVertex;
    }

    public void compute(final Iterable<KryoWritable> messages) {
        if (null == this.tinkerVertex)
            inflateGiraphVertex();
        this.vertexProgram.execute(this.tinkerVertex, new GiraphMessenger(this, messages), this.globals);
        if (this.getConf().getBoolean(GiraphGraphComputer.GREMLIN_DERIVE_GLOBALS, false)) {
            this.globals.keys().forEach(key -> {
                this.tinkerVertex.property(Graph.Key.hidden(key), this.globals.get(key));
            });
            this.tinkerVertex.property(Graph.Key.hidden(GlobalsMapReduce.RUNTIME), this.globals.getRuntime());
            this.tinkerVertex.property(Graph.Key.hidden(GlobalsMapReduce.ITERATION), this.globals.getIteration());
        }
    }

    ///////////////////////////////////////////////

    private Text deflateGiraphVertex() {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final KryoWriter writer = KryoWriter.create().build();
            writer.writeGraph(bos, this.tinkerGraph);
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
            this.tinkerGraph = TinkerGraph.open();
            reader.readGraph(bis, this.tinkerGraph);
            bis.close();
            this.tinkerVertex = (TinkerVertex) this.tinkerGraph.v(this.getId().get());
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
