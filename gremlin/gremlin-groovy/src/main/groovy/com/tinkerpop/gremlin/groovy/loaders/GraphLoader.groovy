package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.blueprints.Graph
import com.tinkerpop.gremlin.groovy.GremlinLoader
import com.tinkerpop.gremlin.pipes.Gremlin

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GraphLoader {

    public static void load() {


        Graph.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return Gremlin.of((Graph) delegate)."$name"();
            } else {
                throw new MissingPropertyException(name, delegate.getClass());
            }
        }

        Graph.metaClass.methodMissing = { final String name, final def args ->
            if (GremlinLoader.isStep(name)) {
                return Gremlin.of((Graph) delegate)."$name"(* args);
            } else {
                throw new MissingMethodException(name, delegate.getClass());
            }
        }

        /*Graph.metaClass.e = { final Object... ids ->
            if (ids.length == 1)
                return ((Graph) delegate).getEdge(ids[0]);
            else {
                final Graph g = (Graph) delegate;
                return new GremlinGroovyPipeline(ids.collect { g.getEdge(it) });
            }
        }*/

    }
}
