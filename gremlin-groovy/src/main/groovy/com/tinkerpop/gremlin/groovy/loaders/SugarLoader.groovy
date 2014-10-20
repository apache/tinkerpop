package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.process.PathTraverser
import com.tinkerpop.gremlin.process.SimpleTraverser
import com.tinkerpop.gremlin.process.Traverser
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.process.util.TraversalHelper
import com.tinkerpop.gremlin.structure.*

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SugarLoader {

    public static void load() {

        GremlinLoader.load();

        [Traverser, PathTraverser, SimpleTraverser].forEach {
            it.metaClass.getProperty = { final String key ->
                TraverserCategory.get((Traverser) delegate, key);
            }
            // g.V.map{it.label()}
            it.metaClass.methodMissing = { final String name, final def args ->
                ((Traverser) delegate).get()."$name"(*args);
            }
        }

        GraphTraversal.metaClass.methodMissing = { final String name, final def args ->
            ((GraphTraversal) delegate).value(name);
        }

        Traverser.metaClass.mixin(TraverserCategory.class);
        GraphTraversal.metaClass.mixin(GraphTraversalCategory.class);
        Graph.metaClass.mixin(GraphCategory.class);
        Vertex.metaClass.mixin(VertexCategory.class);
        Edge.metaClass.mixin(ElementCategory.class);
        VertexProperty.metaClass.mixin(ElementCategory.class);
    }

    public static class TraverserCategory {
        public static final get(final Traverser traverser, final String key) {
            return traverser.sideEffects().exists(key) ? traverser.sideEffects().get(key) : traverser.get()."$key";
        }
    }

    public static class ElementCategory {
        public static final get(final Element element, final String key) {
            final Property property = element.property(key);
            if (property.isPresent())
                return property.value();
            else
                return element."$key"();
        }

        public static final set(final Element element, final String key, final Object value) {
            element.property(key, value);
        }
    }

    public static class VertexCategory {
        public static final get(final Vertex vertex, final String key) {
            final Property property = vertex.property(key);
            if (property.isPresent())
                return property.value();
            else
                return vertex."$key"();
        }

        public static final set(final Vertex vertex, final String key, final Object value) {
            vertex.singleProperty(key, value);
        }

        public static final putAt(final Vertex vertex, final String key, final Object value) {
            vertex.property(key, value);
        }
    }

    public static class GraphCategory {
        private static final String V = "V";
        private static final String E = "E";

        public static final get(final Graph graph, final String key) {
            if (key.equals(V))
                return graph.V();
            else if (key.equals(E))
                return graph.E();
            else
                return graph."$key";
        }
    }

    public static class GraphTraversalCategory {

        public static final get(final GraphTraversal graphTraversal, final String key) {
            // GraphTraversal.metaClass.getMetaMethod(key) ? graphTraversal."$key"() : graphTraversal.value(key);
            graphTraversal."$key"()
        }

        public static final getAt(final GraphTraversal graphTraversal, final Integer index) {
            graphTraversal.range(index, index);
        }

        public static final getAt(final GraphTraversal graphTraversal, final Range range) {
            graphTraversal.range(range.getFrom() as Integer, range.getTo() as Integer);
        }

        public String toString() {
            return TraversalHelper.makeTraversalString(this.metaClass.owner);
        }
    }
}