package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertex extends Vertex<LongWritable, MapWritable, GiraphEdge, Text> {

    Logger logger = Logger.getLogger(GiraphVertex.class);
    private int count = 0;
    private final VertexProgram vertexProgram;

    public GiraphVertex() {
        this.vertexProgram = null;
    }

    public GiraphVertex(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
    }


    public void compute(final Iterable<Text> messages) {

        System.out.println("hello: " + this.count + "---" + this);
        logger.info("Marko Rodriguez!!!!!!!!!!!!!!!");
        if (this.count == 9) {
            this.sendMessage(new LongWritable(1l), new Text("hello"));
        }
        if (this.count++ == 10) {
            MapWritable map = new MapWritable();
            map.put(new Text("marko"), new LongWritable(count));
            this.setValue(map);
            System.out.println("message count: " + StreamFactory.stream(messages).count());
            this.voteToHalt();
        }
    }

}
