package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.GremlinLoader
import com.tinkerpop.gremlin.process.graph.GraphTraversal

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class StepLoader {

    public static void load() {

        GraphTraversal.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"();
            } else {
                return ((GraphTraversal) delegate).value(name);
            }
        }

        /*[Iterable, Iterator].each {
            it.metaClass.count = {
                return PipeHelper.counter(delegate.iterator());
            }
        }*/

        [Iterable, Iterator].each {
            it.metaClass.mean = {
                double counter = 0;
                double sum = 0;
                delegate.each { counter++; sum += it; }
                return sum / counter;
            }
        }

        GraphTraversal.metaClass.getAt = { final Integer index ->
            return ((GraphTraversal) delegate).range(index, index);
        }


        GraphTraversal.metaClass.getAt = { final Range range ->
            return ((GraphTraversal) delegate).range(range.getFrom() as Integer, range.getTo() as Integer);
        }
    }
}
