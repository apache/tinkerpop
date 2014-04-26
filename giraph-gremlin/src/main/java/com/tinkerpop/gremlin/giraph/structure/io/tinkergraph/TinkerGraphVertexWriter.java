package com.tinkerpop.gremlin.giraph.structure.io.tinkergraph;


import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgram;
import com.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphVertexWriter extends VertexWriter {

    GraphWriter writer;

    public void initialize(final TaskAttemptContext context) {
        // this.writer = new GraphSONWriter.Builder().build();
    }

    public void writeVertex(final Vertex giraphVertex) throws IOException {
        /*final Vertex gremlinVertex = ((GiraphVertex) giraphVertex).getGremlinVertex();
        giraphVertex.getPropertyKeys().forEach(key -> {

        });*/

        //System.out.println(((GiraphVertex) giraphVertex).getGremlinVertex() + ":" + ((GiraphVertex) giraphVertex).getGremlinVertex().<TraversalCounters>getProperty(TraversalVertexProgram.TRAVERSAL_TRACKER).get().getDoneObjectTracks());
        System.out.println(((GiraphVertex) giraphVertex).getGremlinVertex() + ":" + ((GiraphVertex) giraphVertex).getGremlinVertex().<Double>getValue(PageRankVertexProgram.PAGE_RANK));
    }

    public void close(final TaskAttemptContext context) {

    }
}
