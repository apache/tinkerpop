package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TinkerFactory {
    public static TinkerGraph createClassic() {
        final TinkerGraph g = TinkerGraph.open();
        generateClassic(g);
        return g;
    }

    public static void generateClassic(final TinkerGraph g) {
        final Vertex marko = g.addVertex(T.id, 1, "name", "marko", "age", 29);
        final Vertex vadas = g.addVertex(T.id, 2, "name", "vadas", "age", 27);
        final Vertex lop = g.addVertex(T.id, 3, "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(T.id, 4, "name", "josh", "age", 32);
        final Vertex ripple = g.addVertex(T.id, 5, "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(T.id, 6, "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5f);
        marko.addEdge("knows", josh, T.id, 8, "weight", 1.0f);
        marko.addEdge("created", lop, T.id, 9, "weight", 0.4f);
        josh.addEdge("created", ripple, T.id, 10, "weight", 1.0f);
        josh.addEdge("created", lop, T.id, 11, "weight", 0.4f);
        peter.addEdge("created", lop, T.id, 12, "weight", 0.2f);
    }

    public static TinkerGraph createModern() {
        final TinkerGraph g = TinkerGraph.open();
        generateModern(g);
        return g;
    }

    public static void generateModern(final TinkerGraph g) {
        final Vertex marko = g.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29);
        final Vertex vadas = g.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27);
        final Vertex lop = g.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32);
        final Vertex ripple = g.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(T.id, 6, T.label, "person", "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5d);
        marko.addEdge("knows", josh, T.id, 8, "weight", 1.0d);
        marko.addEdge("created", lop, T.id, 9, "weight", 0.4d);
        josh.addEdge("created", ripple, T.id, 10, "weight", 1.0d);
        josh.addEdge("created", lop, T.id, 11, "weight", 0.4d);
        peter.addEdge("created", lop, T.id, 12, "weight", 0.2d);
    }

    public static TinkerGraph createTheCrew() {
        final TinkerGraph g = TinkerGraph.open();
        generateTheCrew(g);
        return g;
    }

    public static void generateTheCrew(final TinkerGraph g) {
        final Vertex marko = g.addVertex(T.id, 1, T.label, "person", "name", "marko");
        final Vertex stephen = g.addVertex(T.id, 7, T.label, "person", "name", "stephen");
        final Vertex matthias = g.addVertex(T.id, 8, T.label, "person", "name", "matthias");
        final Vertex daniel = g.addVertex(T.id, 9, T.label, "person", "name", "daniel");
        final Vertex gremlin = g.addVertex(T.id, 10, T.label, "software", "name", "gremlin");
        final Vertex tinkergraph = g.addVertex(T.id, 11, T.label, "software", "name", "tinkergraph");

        marko.property("location", "san diego", "startTime", 1997, "endTime", 2001);
        marko.property("location", "santa cruz", "startTime", 2001, "endTime", 2004);
        marko.property("location", "brussels", "startTime", 2004, "endTime", 2005);
        marko.property("location", "santa fe", "startTime", 2005);

        stephen.property("location", "centreville", "startTime", 1990, "endTime", 2000);
        stephen.property("location", "dulles", "startTime", 2000, "endTime", 2006);
        stephen.property("location", "purcellville", "startTime", 2006);

        matthias.property("location", "bremen", "startTime", 2004, "endTime", 2007);
        matthias.property("location", "baltimore", "startTime", 2007, "endTime", 2011);
        matthias.property("location", "oakland", "startTime", 2011, "endTime", 2014);
        matthias.property("location", "seattle", "startTime", 2014);

        daniel.property("location", "spremberg", "startTime", 1982, "endTime", 2005);
        daniel.property("location", "kaiserslautern", "startTime", 2005, "endTime", 2009);
        daniel.property("location", "aachen", "startTime", 2009);

        marko.addEdge("develops", gremlin, T.id, 13, "since", 2009);
        marko.addEdge("develops", tinkergraph, T.id, 14, "since", 2010);
        marko.addEdge("uses", gremlin, T.id, 15, "skill", 4);
        marko.addEdge("uses", tinkergraph, T.id, 16, "skill", 5);

        stephen.addEdge("develops", gremlin, T.id, 17, "since", 2010);
        stephen.addEdge("develops", tinkergraph, T.id, 18, "since", 2011);
        stephen.addEdge("uses", gremlin, T.id, 19, "skill", 5);
        stephen.addEdge("uses", tinkergraph, T.id, 20, "skill", 4);

        matthias.addEdge("develops", gremlin, T.id, 21, "since", 2012);
        matthias.addEdge("uses", gremlin, T.id, 22, "skill", 3);
        matthias.addEdge("uses", tinkergraph, T.id, 23, "skill", 3);

        daniel.addEdge("uses", gremlin, T.id, 24, "skill", 5);
        daniel.addEdge("uses", tinkergraph, T.id, 25, "skill", 3);

        gremlin.addEdge("traverses", tinkergraph, T.id, 26);

        g.variables().set("creator", "marko");
        g.variables().set("lastModified", 2014);
        g.variables().set("comment", "this graph was created to provide examples and test coverage for tinkerpop3 api advances");
    }

    public interface SocialTraversal<S, E> extends Traversal<S, E> {

        public SocialTraversal<S, Vertex> people(final String name);

        public default SocialTraversal<S, Vertex> knows() {
            final FlatMapStep<Vertex, Vertex> flatMapStep = new FlatMapStep<>(this);
            flatMapStep.setFunction(v -> v.get().out("knows"));
            return (SocialTraversal) this.asAdmin().addStep(flatMapStep);
        }

        public default SocialTraversal<S, Vertex> created() {
            final FlatMapStep<Vertex, Vertex> flatMapStep = new FlatMapStep<>(this);
            flatMapStep.setFunction(v -> v.get().out("created"));
            return (SocialTraversal) this.asAdmin().addStep(flatMapStep);
        }

        public default SocialTraversal<S, String> name() {
            MapStep<Vertex, String> mapStep = new MapStep<>(this);
            mapStep.setFunction(v -> v.get().<String>value("name"));
            return (SocialTraversal) this.asAdmin().addStep(mapStep);
        }

        public static <S> SocialTraversal<S, S> of(final Graph graph) {
            return new DefaultSocialTraversal<>(graph);
        }

        public class DefaultSocialTraversal<S, E> extends DefaultTraversal<S, E> implements SocialTraversal<S, E> {
            private final Graph graph;

            public DefaultSocialTraversal(final Graph graph) {
                super(Graph.class); // should be SocialGraph.class, SocialVertex.class, etc. Perhaps...
                this.graph = graph;
            }

            public SocialTraversal<S, Vertex> people(final String name) {
                return (SocialTraversal) this.asAdmin().addStep(new StartStep<>(this, this.graph.V().has("name", name)));
            }

        }
    }
}
