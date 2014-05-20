package com.tinkerpop.gremlin.giraph.structure.io.tinkergraph;


import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalCounters;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphVertexWriter extends VertexWriter {

    // private GraphWriter writer;
    // private OutputStream outputStream;

    @Override
    public void initialize(final TaskAttemptContext context) throws IOException {
        //this.writer = GraphSONWriter.create().build();
        //final FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        //this.outputStream = fileSystem.create(new Path(context.getConfiguration().get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION)));

    }

    @Override
    public void writeVertex(final Vertex giraphVertex) throws IOException {
        System.out.println(((GiraphVertex) giraphVertex).getGremlinVertex() + ":" + ((GiraphVertex) giraphVertex).getGremlinVertex().<TraversalCounters>property(TraversalVertexProgram.TRAVERSAL_TRACKER).value().getDoneObjectTracks());
    }

    @Override
    public void close(final TaskAttemptContext context) throws IOException {
        //this.outputStream.flush();
        //this.outputStream.close();
    }

    @Override
    public void setConf(final ImmutableClassesGiraphConfiguration configuration) {

    }
}
