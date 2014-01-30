package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerFactory {
    public static TinkerGraph createClassic() {
        final TinkerGraph g = TinkerGraph.open();

        final Vertex marko = g.addVertex(Property.Key.ID, 1, "name", "marko", "age", 29);
        final Vertex vadas = g.addVertex(Property.Key.ID, 2, "name", "vadas", "age", 27);
        final Vertex lop = g.addVertex(Property.Key.ID, 3, "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(Property.Key.ID, 4, "name", "josh", "age", 32);
        final Vertex ripple = g.addVertex(Property.Key.ID, 5, "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(Property.Key.ID, 6, "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, Property.Key.ID, 7, "weight", 0.5f);
        marko.addEdge("knows", josh, Property.Key.ID, 8, "weight", 1.0f);
        marko.addEdge("created", lop, Property.Key.ID, 9, "weight", 0.4f);
        josh.addEdge("created", ripple, Property.Key.ID, 10, "weight", 1.0f);
        josh.addEdge("created", lop, Property.Key.ID, 11, "weight", 0.4f);
        peter.addEdge("created", lop, Property.Key.ID, 12, "weight", 0.2f);

        return g;
    }

    public static TinkerGraph createModern() {
        final TinkerGraph g = TinkerGraph.open();

        final Vertex marko = g.addVertex(Property.Key.ID, 1, "name", "marko", "locations", AnnotatedList.make());
        final Vertex stephen = g.addVertex(Property.Key.ID, 7, "name", "stephen", "locations", AnnotatedList.make());
        final Vertex daniel = g.addVertex(Property.Key.ID, 8, "name", "daniel", "locations", AnnotatedList.make());
        final Vertex matthias = g.addVertex(Property.Key.ID, 9, "name", "matthias", "locations", AnnotatedList.make());

        final AnnotatedList<String> locations = marko.getValue("locations");
        locations.addValue("california", "startTime", 1997, "endTime", 2004);
        locations.addValue("belgium", "startTime", 2004, "endTime", 2005);
        locations.addValue("new mexico", "startTime", 2005, "endTime", 2014);

        return g;
    }

    public static void main(String[] args) {
        final Graph g = TinkerFactory.createModern();
        AnnotatedList<String> locations = g.v(1).get().getValue("locations");
        locations.query().has("value", "california").annotatedValues().forEach(System.out::println);
    }
}
