package com.tinkerpop.gremlin.giraph.structure.util;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphMemory;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphMessenger;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.KryoWritable;
import com.tinkerpop.gremlin.giraph.process.computer.util.RuleWritable;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Direction;
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
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphInternalVertex extends Vertex<LongWritable, Text, NullWritable, KryoWritable> {

    //TODO: Dangerous that the underlying TinkerGraph Vertex can have edges written to it.

    private static final String VERTEX_ID = Graph.Key.hide("vertexId");
    private VertexProgram vertexProgram;
    private TinkerGraph tinkerGraph;
    private TinkerVertex tinkerVertex;
    private GiraphMemory memory;

    public GiraphInternalVertex() {
    }

    public GiraphInternalVertex(final TinkerVertex tinkerVertex) {
        this.tinkerGraph = TinkerGraph.open();
        this.tinkerVertex = tinkerVertex;
        this.tinkerGraph.variables().set(VERTEX_ID, this.tinkerVertex.id());
        final TinkerVertex vertex = (TinkerVertex) this.tinkerGraph.addVertex(T.id, this.tinkerVertex.id(), T.label, this.tinkerVertex.label());
        this.tinkerVertex.iterators().propertyIterator().forEachRemaining(property -> vertex.<Object>property(property.key(), property.value(), T.id, property.id()));
        this.tinkerVertex.iterators().hiddenPropertyIterator().forEachRemaining(property -> vertex.<Object>property(Graph.Key.hide(property.key()), property.value(), T.id, property.id()));
        this.tinkerVertex.iterators().edgeIterator(Direction.OUT, Integer.MAX_VALUE).forEachRemaining(edge -> {
            final com.tinkerpop.gremlin.structure.Vertex tempOtherVertex = edge.iterators().vertexIterator(Direction.IN).next();
            final TinkerVertex otherVertex = (TinkerVertex) ElementHelper.getOrAddVertex(this.tinkerGraph, tempOtherVertex.id(), tempOtherVertex.label());
            final TinkerEdge tinkerEdge = (TinkerEdge) vertex.addEdge(edge.label(), otherVertex, T.id, edge.id());
            edge.iterators().propertyIterator().forEachRemaining(property -> tinkerEdge.<Object>property(property.key(), property.value()));
            edge.iterators().hiddenPropertyIterator().forEachRemaining(property -> tinkerEdge.<Object>property(Graph.Key.hide(property.key()), property.value()));
        });
        this.tinkerVertex.iterators().edgeIterator(Direction.IN, Integer.MAX_VALUE).forEachRemaining(edge -> {
            final com.tinkerpop.gremlin.structure.Vertex tempOtherVertex = edge.iterators().vertexIterator(Direction.OUT).next();
            final TinkerVertex otherVertex = (TinkerVertex) ElementHelper.getOrAddVertex(this.tinkerGraph, tempOtherVertex.id(), tempOtherVertex.label());
            final TinkerEdge tinkerEdge = (TinkerEdge) otherVertex.addEdge(edge.label(), vertex, T.id, edge.id());
            edge.iterators().propertyIterator().forEachRemaining(property -> tinkerEdge.<Object>property(property.key(), property.value()));
            edge.iterators().hiddenPropertyIterator().forEachRemaining(property -> tinkerEdge.<Object>property(Graph.Key.hide(property.key()), property.value()));
        });
        this.initialize(new LongWritable(Long.valueOf(this.tinkerVertex.id().toString())), this.deflateTinkerVertex(), EmptyOutEdges.instance());
        // TODO? this.tinkerVertex = vertex;
    }

    public TinkerVertex getTinkerVertex() {
        return this.tinkerVertex;
    }

    @Override
    public void compute(final Iterable<KryoWritable> messages) {
        if (null == this.tinkerVertex)
            inflateTinkerVertex();
        if (null == this.vertexProgram)
            this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(this.getConf()));
        if (null == this.memory)
            this.memory = new GiraphMemory(this, this.vertexProgram);

        if (!(Boolean) ((RuleWritable) this.getAggregatedValue(Constants.GREMLIN_HALT)).getObject())
            this.vertexProgram.execute(this.tinkerVertex, new GiraphMessenger(this, messages), this.memory);  // TODO provide a wrapper around TinkerVertex for Edge and non-ComputeKeys manipulation
        else if (this.getConf().getBoolean(Constants.GREMLIN_DERIVE_MEMORY, false)) {
            final Map<String, Object> memoryMap = new HashMap<>(this.memory.asMap());
            memoryMap.put(Constants.SYSTEM_ITERATION, this.memory.getIteration() - 1);
            this.tinkerVertex.singleProperty(Constants.MEMORY_MAP, memoryMap);
        }
    }

    ///////////////////////////////////////////////

    private Text deflateTinkerVertex() {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeGraph(bos, this.tinkerGraph);
            bos.flush();
            bos.close();
            return new Text(bos.toByteArray());
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void inflateTinkerVertex() {
        try {
            final ByteArrayInputStream bis = new ByteArrayInputStream(this.getValue().getBytes());
            final KryoReader reader = KryoReader.build().create();
            this.tinkerGraph = TinkerGraph.open();
            reader.readGraph(bis, this.tinkerGraph);
            bis.close();
            this.tinkerVertex = (TinkerVertex) this.tinkerGraph.v(this.tinkerGraph.variables().get(VERTEX_ID).get());
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
