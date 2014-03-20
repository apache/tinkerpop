package com.tinkerpop.gremlin.giraph.structure.io.tinkergraph;


import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalCounters;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphVertexWriter extends VertexWriter {

    public void initialize(final TaskAttemptContext context) {

    }

    public void writeVertex(final Vertex giraphVertex) throws IOException {
        // new GraphSONWriter.Builder().build().writeVertex(System.out, ((GiraphVertex) giraphVertex).getGremlinVertex());
        System.out.println(((GiraphVertex) giraphVertex).getGremlinVertex() + ":" + ((GiraphVertex) giraphVertex).getGremlinVertex().<TraversalCounters>getProperty(TraversalVertexProgram.TRAVERSAL_TRACKER).get().getDoneObjectTracks());
    }

    public void close(final TaskAttemptContext context) {

    }
}
