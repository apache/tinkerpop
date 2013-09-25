package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerFactory {
    public static TinkerGraph createClassic() {
        TinkerGraph graph = new TinkerGraph();

        Vertex marko = graph.addVertex(Property.of(Property.Key.ID, 1, "name", "marko", "age", 29));
        Vertex vadas = graph.addVertex(Property.of(Property.Key.ID, 2, "name", "vadas", "age", 27));
        Vertex lop = graph.addVertex(Property.of(Property.Key.ID, 3, "name", "lop", "lang", "java"));
        Vertex josh = graph.addVertex(Property.of(Property.Key.ID, 4, "name", "josh", "age", 32));
        Vertex ripple = graph.addVertex(Property.of(Property.Key.ID, 5, "name", "ripple", "lang", "java"));
        Vertex peter = graph.addVertex(Property.of(Property.Key.ID, 6, "name", "peter", "age", 35));
        marko.addEdge("knows", vadas, Property.of(Property.Key.ID, 7, "weight", 0.5f));
        marko.addEdge("knows", josh, Property.of(Property.Key.ID, 8, "weight", 1.0f));
        marko.addEdge("created", lop, Property.of(Property.Key.ID, 9, "weight", 0.4f));
        josh.addEdge("created", ripple, Property.of(Property.Key.ID, 10, "weight", 1.0f));
        josh.addEdge("created", lop, Property.of(Property.Key.ID, 11, "weight", 0.4f));
        peter.addEdge("created", lop, Property.of(Property.Key.ID, 12, "weight", 0.2f));

        return graph;
    }
}
