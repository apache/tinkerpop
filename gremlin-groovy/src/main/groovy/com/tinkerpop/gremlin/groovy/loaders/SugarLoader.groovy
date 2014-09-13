package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.process.PathTraverser
import com.tinkerpop.gremlin.process.SimpleTraverser
import com.tinkerpop.gremlin.process.Traverser
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.structure.*
import groovy.lang.MetaProperty

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
        }

        Traverser.metaClass.mixin(TraverserCategory.class);
        GraphTraversal.metaClass.mixin(GraphTraversalCategory.class);
        Graph.metaClass.mixin(GraphCategory.class);
        Vertex.metaClass.mixin(VertexCategory.class);
        Edge.metaClass.mixin(ElementCategory.class);
        MetaProperty.metaClass.mixin(ElementCategory.class);

        // g.V.out.name
        GraphTraversal.metaClass.methodMissing = { final String name, final def args ->
            return ((GraphTraversal) delegate).value(name);
        }

        /* TODO: figure out this one smarty pants
        // g.V.map{it.label()}
        Traverser.metaClass.methodMissing = { final String name, final def args ->
            return ((Traverser) delegate).get()."$name"(*args);
        }*/
    }

    public static class TraverserCategory {
        public static final get(final Traverser traverser, final String key) {
            if (key.equals("loops"))
                return traverser.getLoops();
            else if (key.equals("path"))
                return traverser.getPath();
            else
                return traverser.getSideEffects().exists(key) ? traverser.getSideEffects().get(key) : traverser.get()."$key";
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
        public static final get(final Graph graph, final String key) {
            graph."$key"()
        }
    }

    public static class GraphTraversalCategory {
        public static final get(final GraphTraversal graphTraversal, final String key) {
            graphTraversal."$key"();
        }
    }
}