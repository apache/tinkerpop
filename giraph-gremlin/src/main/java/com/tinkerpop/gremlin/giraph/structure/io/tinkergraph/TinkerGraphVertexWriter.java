package com.tinkerpop.gremlin.giraph.structure.io.tinkergraph;


import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalCounters;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import com.tinkerpop.tinkergraph.structure.TinkerGraph;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphVertexWriter extends VertexWriter {

    GraphSONWriter writer;

    public void initialize(final TaskAttemptContext context) {
        //this.writer = new GraphSONWriter.Builder(TinkerGraph.open()).build();
    }

    public void writeVertex(final Vertex giraphVertex) throws IOException {
        //this.writer.writeVertex(System.out, ((GiraphVertex) giraphVertex).getGremlinVertex());
        System.out.println(((GiraphVertex) giraphVertex).getGremlinVertex() + ":" + ((GiraphVertex) giraphVertex).getGremlinVertex().<TraversalCounters>getProperty(TraversalVertexProgram.TRAVERSAL_TRACKER).get().getDoneObjectTracks());
    }

    public void close(final TaskAttemptContext context) {

    }
}
