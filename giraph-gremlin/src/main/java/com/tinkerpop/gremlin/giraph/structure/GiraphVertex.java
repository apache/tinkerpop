package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.olap.GiraphComputerMemory;
import com.tinkerpop.gremlin.giraph.process.olap.GiraphMessenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgram;
import com.tinkerpop.gremlin.structure.Element;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertex extends Vertex<LongWritable, MapWritable, MapWritable, DoubleWritable> {

    Logger logger = Logger.getLogger(GiraphVertex.class);
    private final VertexProgram vertexProgram;
    private final com.tinkerpop.gremlin.structure.Vertex gremlinVertex;

    public GiraphVertex() {
        this.vertexProgram = null;
        this.gremlinVertex = null;
    }

    public GiraphVertex(final com.tinkerpop.gremlin.structure.Vertex gremlinVertex, final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        this.gremlinVertex = gremlinVertex;
        final MapWritable properties = new MapWritable();
        properties.put(new Text(Element.LABEL), new Text(gremlinVertex.getLabel()));
        gremlinVertex.getProperties().forEach((k, v) -> properties.put(new Text(k), new Text(v.toString())));
        this.initialize(new LongWritable(Long.valueOf(gremlinVertex.getId().toString())), properties, GiraphEdge.createOutEdges(this.gremlinVertex));
    }

    public void compute(final Iterable<DoubleWritable> messages) {
        System.out.println(this.gremlinVertex + ": " + this.gremlinVertex.getProperty(PageRankVertexProgram.PAGE_RANK));
        if (this.vertexProgram.terminate(new GiraphComputerMemory(this))) {
            this.voteToHalt();
        } else {
            this.vertexProgram.execute(this.gremlinVertex, new GiraphMessenger(this, messages), new GiraphComputerMemory(this));
        }
    }

}
