package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.olap.GiraphComputerMemory;
import com.tinkerpop.gremlin.giraph.process.olap.GiraphMessenger;
import com.tinkerpop.gremlin.giraph.structure.util.EmptyOutEdges;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgram;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertex extends Vertex<LongWritable, NullWritable, NullWritable, DoubleWritable> {

    Logger logger = Logger.getLogger(GiraphVertex.class);
    private VertexProgram vertexProgram;
    private com.tinkerpop.gremlin.structure.Vertex gremlinVertex;
    private GiraphComputerMemory computerMemory;

    public GiraphVertex(final com.tinkerpop.gremlin.structure.Vertex gremlinVertex) {
        try {
            this.vertexProgram = (VertexProgram) new ObjectInputStream(new FileInputStream("targets")).readObject();
        } catch (Exception e) {
            java.lang.System.out.println(e.getMessage());
        }
        this.gremlinVertex = gremlinVertex;
        this.computerMemory = new GiraphComputerMemory(this);
        this.initialize(new LongWritable(Long.valueOf(gremlinVertex.getId().toString())), NullWritable.get(), EmptyOutEdges.instance());
    }

    public void compute(final Iterable<DoubleWritable> messages) {
        System.out.println(this.gremlinVertex + ": " + this.gremlinVertex.getProperty(PageRankVertexProgram.PAGE_RANK));
        this.vertexProgram.execute(this.gremlinVertex, new GiraphMessenger(this, messages), this.computerMemory);
    }

}
