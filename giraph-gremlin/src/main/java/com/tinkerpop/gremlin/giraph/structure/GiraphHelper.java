package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphHelper {

    public static Iterator<Vertex> getVertices(final GiraphGraph graph, final Vertex vertex, final Direction direction, final int branchFactor, final String... labels) {
        /*return vertex instanceof GiraphVertex ?
                StreamFactory.stream(((GiraphVertex) vertex).getBaseVertex().iterators().vertices(direction, branchFactor, labels)).map(v -> graph.v(v.id())).iterator() :*/
        return vertex.iterators().vertices(direction, branchFactor, labels);
    }

    public static Iterator<Edge> getEdges(final GiraphGraph graph, final Vertex vertex, final Direction direction, final int branchFactor, final String... labels) {
        /*return vertex instanceof GiraphVertex ?
                (Iterator) StreamFactory.stream(((GiraphVertex) vertex).getBaseVertex().iterators().edges(direction, branchFactor, labels)).map(e -> new GiraphEdge((TinkerEdge) e, graph)).iterator() :*/
        return vertex.iterators().edges(direction, branchFactor, labels);

    }

    public static Iterator<Vertex> getVertices(final GiraphGraph graph, final Edge edge, final Direction direction) {
        /*return edge instanceof GiraphEdge ?
                ((GiraphEdge) edge).getBaseEdge().flatMap(e -> e.get().iterators().vertices(direction)).map(v -> graph.v(v.get().id())) :  */
        return edge.iterators().vertices(direction);
    }

    public static GiraphGraph getOutputGraph(final GiraphGraph giraphGraph) {
        final Configuration conf = new BaseConfiguration();
        giraphGraph.variables().getConfiguration().getKeys().forEachRemaining(key -> {
            //try {
            conf.setProperty(key, giraphGraph.variables().getConfiguration().getProperty(key));
            //} catch (Exception e) {
            // do nothing for serialization problems
            //}
        });
        if (giraphGraph.variables().getConfiguration().containsKey(Constants.GREMLIN_OUTPUT_LOCATION)) {
            conf.setProperty(Constants.GREMLIN_INPUT_LOCATION, giraphGraph.variables().getConfiguration().getOutputLocation() + "/" + Constants.HIDDEN_G);
            conf.setProperty(Constants.GREMLIN_OUTPUT_LOCATION, giraphGraph.variables().getConfiguration().getOutputLocation() + "_");
        }
        if (giraphGraph.variables().getConfiguration().containsKey(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS)) {
            // TODO: Is this sufficient?
            conf.setProperty(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, giraphGraph.variables().getConfiguration().getString(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS).replace("OutputFormat", "InputFormat"));
        }
        return GiraphGraph.open(conf);
    }
}
