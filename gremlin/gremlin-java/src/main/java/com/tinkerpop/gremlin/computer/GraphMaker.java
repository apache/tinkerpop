package com.tinkerpop.gremlin.computer;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphMaker {

    public static Graph makeNeighborGraph(final Vertex vertex) {
        final Graph graph = new TinkerGraph();
        Vertex v1 = makeClone(graph, vertex);
        vertex.query().direction(Direction.OUT).edges().forEach(e -> {
            Vertex v2 = makeClone(graph, e.getVertex(Direction.IN));
            v1.addEdge(e.getLabel(), v2);
        });
        vertex.query().direction(Direction.IN).edges().forEach(e -> {
            Vertex v2 = makeClone(graph, e.getVertex(Direction.OUT));
            v2.addEdge(e.getLabel(), v1);
        });
        return graph;
    }

    private static Vertex makeClone(final Graph graph, final Vertex vertex) {
        final Object id = vertex.getId();
        final Iterator<Vertex> itty = graph.query().ids(id).vertices().iterator();
        if (itty.hasNext()) {
            return itty.next();
        } else {
            Vertex v1 = graph.addVertex(Property.of(Property.Key.ID, id));
            vertex.getPropertyKeys().forEach(s -> v1.setProperty(s, vertex.getValue(s)));
            return v1;
        }
    }
}
