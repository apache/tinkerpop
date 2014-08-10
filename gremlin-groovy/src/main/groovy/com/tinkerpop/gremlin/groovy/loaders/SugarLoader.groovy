package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.process.Traverser
import com.tinkerpop.gremlin.structure.Element

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SugarLoader {

    public static void load() {

        Element.metaClass.propertyMissing = { final String name, final def value ->
            ((Element) delegate).property(name, value)
        }

        Element.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"();
            } else {
                return ((Element) delegate).value(name);
            }
        }

        Traverser.metaClass.propertyMissing = { final String name ->
            return ((Traverser) delegate).get()."$name";
        }

        Traverser.metaClass.methodMissing = { final String name, final def args ->
            return ((Traverser) delegate).get()."$name"(*args);
        }
    }
}


