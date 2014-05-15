package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.GremlinLoader
import com.tinkerpop.gremlin.structure.Element
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GraphLoader {

    public static void load() {

        Graph.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"();
            } else {
                throw new MissingPropertyException(name, delegate.getClass());
            }
        }

        Graph.metaClass.methodMissing = { final String name, final def args ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"(* args);
            } else {
                throw new MissingMethodException(name, delegate.getClass());
            }
        }

        Element.metaClass.getAt = { final String property ->
            return ((Element) delegate).value(property);
        }

        Element.metaClass.getAt = { final String property, final Object orElse ->
            return ((Element) delegate).property(property).orElse(orElse);
        }

        Vertex.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"();
            } else {
                throw new MissingPropertyException(name, delegate.getClass());
            }
        }
    }
}
