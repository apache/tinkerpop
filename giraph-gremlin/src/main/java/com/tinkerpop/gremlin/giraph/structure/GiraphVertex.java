package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.olap.GiraphComputerMemory;
import com.tinkerpop.gremlin.giraph.process.olap.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.olap.GiraphMessenger;
import com.tinkerpop.gremlin.giraph.process.olap.KryoWritable;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalCounterMessage;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalCounters;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertex extends Vertex<LongWritable, NullWritable, NullWritable, KryoWritable> {

    private static final Logger LOGGER = Logger.getLogger(GiraphVertex.class);

    private VertexProgram vertexProgram;
    private com.tinkerpop.gremlin.structure.Vertex gremlinVertex;
    private GiraphComputerMemory computerMemory;

    public GiraphVertex(final com.tinkerpop.gremlin.structure.Vertex gremlinVertex) {
        try {
            this.vertexProgram = (VertexProgram) new ObjectInputStream(new FileInputStream(GiraphGraphComputer.VERTEX_PROGRAM)).readObject();
        } catch (Exception e) {
            java.lang.System.out.println(e + "--->" + e.getMessage());
        }
        this.gremlinVertex = gremlinVertex;
        this.computerMemory = new GiraphComputerMemory(this);
        this.initialize(new LongWritable(Long.valueOf(gremlinVertex.getId().toString())), NullWritable.get(), EmptyOutEdges.instance());
        KryoWritable.tClass = TraversalCounterMessage.class;
    }

    public com.tinkerpop.gremlin.structure.Vertex getGremlinVertex() {
        return this.gremlinVertex;
    }

    public void compute(final Iterable<KryoWritable> messages) {
        //System.out.println(this.gremlinVertex + ": " + this.gremlinVertex.<TraversalPaths>getProperty(TraversalVertexProgram.TRAVERSAL_TRACKER).orElse(new TraversalPaths(this.gremlinVertex)).getDoneObjectTracks());
        System.out.println(this.gremlinVertex + ": " + this.gremlinVertex.<TraversalCounters>getProperty(TraversalVertexProgram.TRAVERSAL_TRACKER).orElse(new TraversalCounters(this.gremlinVertex)).getDoneObjectTracks());
        this.vertexProgram.execute(this.gremlinVertex, new GiraphMessenger(this, messages), this.computerMemory);
    }

}
