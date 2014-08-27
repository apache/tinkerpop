package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;
import com.tinkerpop.gremlin.structure.Element;
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
        final Vertex marko = g.addVertex(Element.ID, 1, Element.LABEL, "person", "name", "marko", "age", 29);
        final Vertex vadas = g.addVertex(Element.ID, 2, Element.LABEL, "person", "name", "vadas", "age", 27);
        final Vertex lop = g.addVertex(Element.ID, 3, Element.LABEL, "software", "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(Element.ID, 4, Element.LABEL, "person", "name", "josh", "age", 32);
        final Vertex ripple = g.addVertex(Element.ID, 5, Element.LABEL, "software", "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(Element.ID, 6, Element.LABEL, "person", "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, Element.ID, 7, "weight", 0.5d);
        marko.addEdge("knows", josh, Element.ID, 8, "weight", 1.0d);
        marko.addEdge("created", lop, Element.ID, 9, "weight", 0.4d);
        josh.addEdge("created", ripple, Element.ID, 10, "weight", 1.0d);
        josh.addEdge("created", lop, Element.ID, 11, "weight", 0.4d);
        peter.addEdge("created", lop, Element.ID, 12, "weight", 0.2d);
    }

    public interface SocialTraversal<S, E> extends Traversal<S, E> {

        public default SocialTraversal<S, Vertex> people() {
            return (SocialTraversal) this.addStep(new StartStep<>(this, this.sideEffects().getGraph().V().has("age")));
        }

        public default SocialTraversal<S, Vertex> people(String name) {
            return (SocialTraversal) this.addStep(new StartStep<>(this, this.sideEffects().getGraph().V().has("name", name)));
        }

        public default SocialTraversal<S, Vertex> knows() {
            final FlatMapStep<Vertex, Vertex> flatMapStep = new FlatMapStep<>(this);
            flatMapStep.setFunction(v -> v.get().out("knows"));
            return (SocialTraversal) this.addStep(flatMapStep);
        }

        public default SocialTraversal<S, Vertex> created() {
            final FlatMapStep<Vertex, Vertex> flatMapStep = new FlatMapStep<>(this);
            flatMapStep.setFunction(v -> v.get().out("created"));
            return (SocialTraversal) this.addStep(flatMapStep);
        }

        public default SocialTraversal<S, String> name() {
            MapStep<Vertex, String> mapStep = new MapStep<>(this);
            mapStep.setFunction(v -> v.get().<String>value("name"));
            return (SocialTraversal) this.addStep(mapStep);
        }

        public static <S> SocialTraversal<S, S> of(final Graph graph) {
            final SocialTraversal traversal = new DefaultSocialTraversal();
            traversal.sideEffects().setGraph(graph);
            return traversal;
        }

        public class DefaultSocialTraversal extends DefaultTraversal implements SocialTraversal {

        }
    }
}
