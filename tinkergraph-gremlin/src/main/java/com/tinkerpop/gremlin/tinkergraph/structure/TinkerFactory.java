package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.map.MapStep;
import com.tinkerpop.gremlin.process.graph.map.StartStep;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerFactory {
    public static TinkerGraph createClassic() {
        final TinkerGraph g = TinkerGraph.open();
		generateClassic(g);
		return g;
    }

	public static void generateClassic(final TinkerGraph g) {
		final Vertex marko = g.addVertex(Element.ID, 1, "name", "marko", "age", 29);
		final Vertex vadas = g.addVertex(Element.ID, 2, "name", "vadas", "age", 27);
		final Vertex lop = g.addVertex(Element.ID, 3, "name", "lop", "lang", "java");
		final Vertex josh = g.addVertex(Element.ID, 4, "name", "josh", "age", 32);
		final Vertex ripple = g.addVertex(Element.ID, 5, "name", "ripple", "lang", "java");
		final Vertex peter = g.addVertex(Element.ID, 6, "name", "peter", "age", 35);
		marko.addEdge("knows", vadas, Element.ID, 7, "weight", 0.5f);
		marko.addEdge("knows", josh, Element.ID, 8, "weight", 1.0f);
		marko.addEdge("created", lop, Element.ID, 9, "weight", 0.4f);
		josh.addEdge("created", ripple, Element.ID, 10, "weight", 1.0f);
		josh.addEdge("created", lop, Element.ID, 11, "weight", 0.4f);
		peter.addEdge("created", lop, Element.ID, 12, "weight", 0.2f);
	}

    public static TinkerGraph createModern() {
        final TinkerGraph g = TinkerGraph.open();
		generateModern(g);
        return g;
    }

	public static void generateModern(final TinkerGraph g) {
		// todo: need to add hidden properties to make sure IO works well for these items
		final Graph.Variables variables = g.variables();
		variables.set("name", "modern");
		variables.set("year", 2014);

		final Vertex marko = g.addVertex(Element.ID, 1, Element.LABEL, "person", "name", "marko", "locations", AnnotatedList.make());
		final Vertex stephen = g.addVertex(Element.ID, 7, Element.LABEL, "person", "name", "stephen", "locations", AnnotatedList.make());
		final Vertex matthias = g.addVertex(Element.ID, 8, Element.LABEL, "person", "name", "matthias", "locations", AnnotatedList.make());
		final Vertex daniel = g.addVertex(Element.ID, 9, Element.LABEL, "person", "name", "daniel", "locations", AnnotatedList.make());
		final Vertex gremlin = g.addVertex(Element.ID, 10, Element.LABEL, "software", "name", "gremlin");
		final Vertex blueprints = g.addVertex(Element.ID, 11, Element.LABEL, "software", "name", "blueprints");

		AnnotatedList<String> locations = marko.value("locations");
		locations.addValue("san diego", "startTime", 1997, "endTime", 2001);
		locations.addValue("santa cruz", "startTime", 2001, "endTime", 2004);
		locations.addValue("brussels", "startTime", 2004, "endTime", 2005);
		locations.addValue("santa fe", "startTime", 2005, "endTime", 2014);

		locations = stephen.value("locations");
		locations.addValue("centreville", "startTime", 1990, "endTime", 2000);
		locations.addValue("dulles", "startTime", 2000, "endTime", 2006);
		locations.addValue("purcellville", "startTime", 2006, "endTime", 2014);

		locations = matthias.value("locations");
		locations.addValue("bremen", "startTime", 2004, "endTime", 2007);
		locations.addValue("baltimore", "startTime", 2007, "endTime", 2011);
		locations.addValue("oakland", "startTime", 2011, "endTime", 2014);

		locations = daniel.value("locations");
		locations.addValue("spremberg", "startTime", 1982, "endTime", 2005);
		locations.addValue("kaiserslautern", "startTime", 2005, "endTime", 2009);
		locations.addValue("aachen", "startTime", 2009, "endTime", 2014);

		marko.addEdge("created", gremlin, "date", 2009);
		marko.addEdge("uses", gremlin, "skill", 0.9f);
		marko.addEdge("created", blueprints, "date", 2010);
		stephen.addEdge("created", blueprints, "date", 2011);
		stephen.addEdge("created", gremlin, "date", 2014);
		matthias.addEdge("created", blueprints, "date", 2012);
		daniel.addEdge("uses", gremlin, "skill", 1.0f);
		gremlin.addEdge("dependsOn", blueprints);
	}

    public interface SocialTraversal<S, E> extends Traversal<S, E> {

        public default SocialTraversal<S, Vertex> people() {
            return (SocialTraversal) this.addStep(new StartStep<Vertex>(this, this.memory().<Graph>get("g").V().has("age")));
        }

        public default SocialTraversal<S, Vertex> people(String name) {
            return (SocialTraversal) this.addStep(new StartStep<Vertex>(this, this.memory().<Graph>get("g").V().has("name", name)));
        }

        public default SocialTraversal<S, Vertex> knows() {
            final FlatMapStep<Vertex, Vertex> flatMapStep = new FlatMapStep<>(this);
            flatMapStep.setFunction(v -> v.get().out("knows"));
            return (SocialTraversal) this.addStep(flatMapStep);
        }

        public default SocialTraversal<S, String> name() {
            MapStep<Vertex, String> mapStep = new MapStep<>(this);
            mapStep.setFunction(v -> v.get().<String>value("name"));
            return (SocialTraversal) this.addStep(mapStep);
        }

        public static SocialTraversal of() {
            return new DefaultSocialTraversal();
        }

        public class DefaultSocialTraversal extends DefaultTraversal implements SocialTraversal {

        }
    }
}
