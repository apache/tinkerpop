package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.Traverser
import com.tinkerpop.gremlin.process.graph.traversal.GraphTraversal
import com.tinkerpop.gremlin.process.graph.traversal.__
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import com.tinkerpop.gremlin.structure.*

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SugarLoader {

    private static final String NAME = "name";
    private static final String FROM = "from";
    private static final String SELECT = "select";

    public static void load() {

        GremlinLoader.load();

        Traverser.metaClass.getProperty = { final String key ->
            TraverserCategory.get((Traverser) delegate, key);
        }
        // g.V.map{it.label()}
        Traverser.metaClass.methodMissing = { final String name, final def args ->
            ((Traverser) delegate).get()."$name"(*args);
        }
        // g.V.age
        GraphTraversal.metaClass.methodMissing = { final String name, final def args ->
            if (name.equals(FROM))
                return ((GraphTraversal.Admin) args[0]).addStep(((GraphTraversal.Admin) delegate).getSteps()[0]);
            else if (Compare.hasCompare(name))
                return ((GraphTraversal) delegate).is(Compare.valueOf(name), *args);
            else
                return ((GraphTraversal) delegate).values(name);
        }
        // __.age and __.out
        __.metaClass.static.propertyMissing = { final String name ->
            return null != __.metaClass.getMetaMethod(name) ? __."$name"() : __.values(name);
        }
        // out and age
        /*Object.metaClass.propertyMissing = { final String name ->
            if (name.equals(NAME))
                return __.values(NAME);
            else
                return __."$name";
        }*/

        // select x,y from ...
        Object.metaClass.methodMissing = { final String name, final def args ->
            if (name.equals(SELECT)) return __.select(*args)
            throw new MissingMethodException(name, delegate.getClass(), args);
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
            return traverser.get()."$key";
        }
    }

    public static class ElementCategory {
        public static final Object get(final Element element, final String key) {
            // TODO: Weird:::: return element.property(key).orElseGet{vertex."$key"()};
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
        public static final Object get(final Vertex vertex, final String key) {
            // TODO: Weird:::: return vertex.property(key).orElseGet{vertex."$key"()};
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
            graphTraversal."$key"()
        }

        public static final getAt(final GraphTraversal graphTraversal, final Integer index) {
            graphTraversal.range(index, index + 1);
        }

        public static final getAt(final GraphTraversal graphTraversal, final Range range) {
            graphTraversal.range(range.getFrom() as Integer, range.getTo() as Integer);
        }

        public static final or(final GraphTraversal.Admin leftTraversal, final Traversal.Admin rightTraversal) {
            leftTraversal.or();
            rightTraversal.getSteps().forEach { step ->
                leftTraversal.addStep(step);
            }
            return leftTraversal;
        }

        public static final and(final GraphTraversal.Admin leftTraversal, final Traversal.Admin rightTraversal) {
            leftTraversal.and();
            rightTraversal.getSteps().forEach { step ->
                leftTraversal.addStep(step);
            }
            return leftTraversal;
        }

        public String toString() {
            return TraversalHelper.makeTraversalString(this.metaClass.owner);
        }
    }
}