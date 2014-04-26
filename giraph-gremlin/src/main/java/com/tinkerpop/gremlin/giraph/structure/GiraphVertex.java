package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.olap.GiraphComputerMemory;
import com.tinkerpop.gremlin.giraph.process.olap.GiraphMessenger;
import com.tinkerpop.gremlin.giraph.process.olap.KryoWritable;
import com.tinkerpop.gremlin.giraph.process.olap.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertex extends Vertex<LongWritable, NullWritable, NullWritable, KryoWritable> {

    private static final Logger LOGGER = Logger.getLogger(GiraphVertex.class);

    private VertexProgram vertexProgram;
    private com.tinkerpop.gremlin.structure.Vertex gremlinVertex;
    private GiraphComputerMemory computerMemory;

    public GiraphVertex(final Configuration hadoopConfiguration, final com.tinkerpop.gremlin.structure.Vertex gremlinVertex) {
        this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.apacheConfiguration(hadoopConfiguration));
        this.gremlinVertex = gremlinVertex;
        this.computerMemory = new GiraphComputerMemory(this);
        this.initialize(new LongWritable(Long.valueOf(gremlinVertex.getId().toString())), NullWritable.get(), EmptyOutEdges.instance());
        KryoWritable.tClass = this.vertexProgram.getMessageClass();
    }

    public com.tinkerpop.gremlin.structure.Vertex getGremlinVertex() {
        return this.gremlinVertex;
    }

    public void compute(final Iterable<KryoWritable> messages) {
        this.vertexProgram.execute(this.gremlinVertex, new GiraphMessenger(this, messages), this.computerMemory);
    }

}
