package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.process.Traverser
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.structure.Element
import com.tinkerpop.gremlin.structure.Graph

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SugarLoader {

    private static final String V = "V";
    private static final String E = "E";

    public static void load() {

        // g.V, g.E
        Graph.metaClass.propertyMissing = { final String name ->
            if (name.equals(V))
                return ((Graph) delegate).V();
            else if (name.equals(E))
                return ((Graph) delegate).E();
            else
                throw new MissingPropertyException(name, delegate.getClass());
        }

        // v.name = marko
        Element.metaClass.propertyMissing = { final String name, final def value ->
            ((Element) delegate).property(name, value)
        }

        // v.name
        Element.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"();
            } else {
                return ((Element) delegate).value(name);
            }
        }

        // g.V.out.name
        GraphTraversal.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"();
            } else {
                return ((GraphTraversal) delegate).value(name);
            }
        }

        // g.V.map{it.name}
        Traverser.metaClass.propertyMissing = { final String name ->
            return ((Traverser) delegate).get()."$name";
        }

        // g.V.map{it.label()}
        Traverser.metaClass.methodMissing = { final String name, final def args ->
            return ((Traverser) delegate).get()."$name"(*args);
        }
    }
}


